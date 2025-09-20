use std::{
    collections::VecDeque,
    mem::replace,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rocksdb::DB;
use tokio::{
    sync::{
        mpsc::{Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{network::NetworkId, storage::plain::PlainPrefetchStorage};

use super::{
    NodeIndex, StateVersion, StorageConfig, StorageKey, StorageOp,
    network::Storage,
    plain::{PlainStorage, PlainSyncStorage},
};

pub struct BenchConfig {
    num_key: u64,
    put_ratio: f64,
    prefetch_offset: StateVersion,
}

pub struct Bench {
    config: BenchConfig,

    cancel: CancellationToken,
    tx_op: Sender<StorageOp>,

    rng: StdRng,
    command_queue: VecDeque<(Command, StorageKey)>,
    records: Vec<(Instant, Duration)>,
}

enum Command {
    Get,
    Put,
}

impl Bench {
    pub fn new(config: BenchConfig, cancel: CancellationToken, tx_op: Sender<StorageOp>) -> Self {
        Self {
            config,
            cancel,
            tx_op,
            rng: StdRng::seed_from_u64(117418),
            command_queue: Default::default(),
            records: Default::default(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        if let (Some(first), Some(last)) = (self.records.first(), self.records.last()) {
            let total = last.0.duration_since(first.0) + last.1;
            let ops = self.records.len() as f64;
            let tps = ops / total.as_secs_f64();
            let mean = self.records.iter().map(|r| r.1).sum::<Duration>() / ops as u32;
            println!("ops: {ops}, total: {total:.1?}, tps: {tps:.2}, mean latency: {mean:?}")
        }
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        for _ in 0..=self.config.prefetch_offset {
            self.push_command().await?
        }
        let mut interval_start = Instant::now();
        let mut interval_start_num_record = 0;
        loop {
            let start;
            let Some((command, key)) = self.command_queue.pop_front() else {
                unreachable!()
            };
            let updates = match command {
                Command::Put => {
                    let mut value = vec![0; 68];
                    self.rng.fill(&mut value[..]);
                    start = Instant::now();
                    vec![(key, Some(value.into()))]
                }
                Command::Get => {
                    start = Instant::now();
                    let (tx_value, rx_value) = oneshot::channel();
                    let _ = self.tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                    let value = rx_value.await?;
                    anyhow::ensure!(value.is_some(), "key not found");
                    Default::default()
                }
            };
            let (tx_ok, rx_ok) = oneshot::channel();
            let _ = self.tx_op.send(StorageOp::Bump(updates, tx_ok)).await;
            rx_ok.await?;
            let duration = start.elapsed();
            self.records.push((start, duration));

            self.push_command().await?;

            let now = Instant::now();
            let elapsed = now.duration_since(interval_start);
            if elapsed >= Duration::from_secs(1) {
                interval_start = now;
                let interval_num_record = self.records.len()
                    - replace(&mut interval_start_num_record, self.records.len());
                let tps = interval_num_record as f64 / elapsed.as_secs_f64();
                info!("interval tps: {tps:.2}")
            }
        }
    }

    async fn push_command(&mut self) -> Result<(), anyhow::Error> {
        let command = if self.rng.random_bool(self.config.put_ratio) {
            Command::Put
        } else {
            Command::Get
        };
        let key = Self::uniform_key(self.rng.random_range(0..self.config.num_key));
        if matches!(command, Command::Get) {
            let (tx_ok, rx_ok) = oneshot::channel();
            let _ = self.tx_op.send(StorageOp::Prefetch(key, tx_ok)).await;
            // rx_ok.await?;
            drop(rx_ok);
        }
        self.command_queue.push_back((command, key));
        Ok(())
    }

    fn uniform_key(index: u64) -> StorageKey {
        StorageKey(StdRng::seed_from_u64(index).random())
    }

    pub fn prefill_items(config: BenchConfig) -> impl Iterator<Item = (StorageKey, Bytes)> {
        let mut rng = StdRng::seed_from_u64(117418);
        (0..config.num_key).map(move |i| {
            let key = Self::uniform_key(i);
            let mut value = vec![0; 68];
            rng.fill(&mut value[..]);
            (key, Bytes::from(value))
        })
    }
}

pub struct BenchPlainStorage {
    bench: Bench,
    plain_storage: PlainStorage,
}

impl BenchPlainStorage {
    pub fn new(
        prefetch: bool,
        config: BenchConfig,
        db: Arc<DB>,
        cancel: CancellationToken,
    ) -> Self {
        let (tx_op, rx_op) = channel(1);
        let plain_storage = if prefetch {
            PlainStorage::Prefetch(PlainPrefetchStorage::new(
                db,
                config.prefetch_offset,
                cancel.clone(),
                rx_op,
            ))
        } else {
            PlainStorage::Sync(PlainSyncStorage::new(db, cancel.clone(), rx_op))
        };
        let bench = Bench::new(config, cancel, tx_op);
        Self {
            bench,
            plain_storage,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        try_join!(self.plain_storage.run(), self.bench.run())?;
        Ok(())
    }
}

pub struct BenchStorage {
    bench: Bench,
    storage: Storage,
}

impl BenchStorage {
    pub fn new(
        network_id: NetworkId,
        addrs: Vec<SocketAddr>,
        bench_config: BenchConfig,
        storage_config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        node_table: Vec<NetworkId>,
        cancel: CancellationToken,
        tx_start: oneshot::Sender<()>,
    ) -> Self {
        let (tx_op, rx_op) = channel(1);

        let bench = Bench::new(bench_config, cancel.clone(), tx_op);
        let storage = Storage::new(
            storage_config,
            node_indices,
            db,
            node_table,
            addrs,
            network_id,
            cancel,
            rx_op,
            tx_start,
        );
        Self { bench, storage }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        try_join!(self.storage.run(), self.bench.run())?;
        Ok(())
    }
}

mod parse {
    use crate::parse::Extract;

    use super::BenchConfig;

    impl Extract for BenchConfig {
        fn extract(configs: &crate::parse::Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_key: configs.get("bench.num-key")?,
                put_ratio: configs.get("bench.put-ratio")?,
                prefetch_offset: configs.get("bench.prefetch-offset")?,
            })
        }
    }
}
