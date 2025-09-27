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
    spawn,
    sync::{
        mpsc::{Sender, channel},
        oneshot,
    },
    task::JoinHandle,
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{network::NetworkId, storage::plain::PlainPrefetchStorage};

use super::{
    BumpUpdates, NodeIndex, StateVersion, StorageConfig, StorageKey, StorageOp,
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

    tx_op: Sender<StorageOp>,

    rng: StdRng,
    command_queue: VecDeque<Command>,
    records: Vec<(Instant, Duration, Duration)>,
}

struct Command {
    start: Instant,
    task: JoinHandle<anyhow::Result<Duration>>,
    updates: BumpUpdates,
}

impl Bench {
    pub fn new(config: BenchConfig, tx_op: Sender<StorageOp>) -> Self {
        Self {
            config,
            tx_op,
            rng: StdRng::seed_from_u64(117418),
            command_queue: Default::default(),
            records: Default::default(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        for command in self.command_queue {
            command.task.abort()
        }
        if let (Some(first), Some(last)) = (self.records.first(), self.records.last()) {
            let total = last.0.duration_since(first.0) + last.1;
            let ops = self.records.len() as f64;
            let tps = ops / total.as_secs_f64();
            let fetch_mean = self.records.iter().map(|r| r.1).sum::<Duration>() / ops as u32;
            let bump_mean = self.records.iter().map(|r| r.2).sum::<Duration>() / ops as u32;
            println!(
                "ops: {ops}, total: {total:.1?}, tps: {tps:.2}, mean latency: fetch {fetch_mean:?} bump {bump_mean:?}"
            )
        } else {
            println!("no operations performed")
        }
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        for _ in 0..=self.config.prefetch_offset {
            self.push_command()
        }
        let mut interval_start = Instant::now();
        let mut interval_start_num_record = 0;
        loop {
            let Some(command) = self.command_queue.pop_front() else {
                unreachable!()
            };
            let Ok(Ok(fetch_latency)) = command.task.await else {
                break;
            };
            let start = Instant::now();
            if !command.updates.is_empty() {
                let (tx_ok, rx_ok) = oneshot::channel();
                let _ = self
                    .tx_op
                    .send(StorageOp::Bump(command.updates, tx_ok))
                    .await;
                let Ok(()) = rx_ok.await else {
                    break;
                };
            }
            self.records
                .push((command.start, fetch_latency, start.elapsed()));
            self.push_command();

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
        Ok(())
    }

    fn push_command(&mut self) {
        enum Workload {
            Put(Bytes),
            Get,
        }
        let key = Self::uniform_key(self.rng.random_range(0..self.config.num_key));
        let workload = if self.rng.random_bool(self.config.put_ratio) {
            let mut bytes = vec![0; 68];
            self.rng.fill(&mut bytes[..]);
            Workload::Put(bytes.into())
        } else {
            Workload::Get
        };
        let tx_op = self.tx_op.clone();
        let start = Instant::now();
        let (task, updates) = match workload {
            Workload::Get => {
                let task = spawn(async move {
                    let start = Instant::now();
                    let (tx_value, rx_value) = oneshot::channel();
                    let _ = tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                    rx_value.await?;
                    Ok(start.elapsed())
                });
                (task, Default::default())
            }
            Workload::Put(value) => (
                spawn(async move { Ok(Duration::ZERO) }),
                vec![(key, Some(value))],
            ),
        };
        self.command_queue.push_back(Command {
            start,
            task,
            updates,
        });
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
            (key, value.into())
        })
    }
}

pub struct BenchPlainStorage {
    bench: Bench,
    plain_storage: PlainStorage,
}

impl BenchPlainStorage {
    pub fn new(prefetch: bool, config: BenchConfig, db: Arc<DB>) -> Self {
        let (tx_op, rx_op) = channel(1);
        let plain_storage = if prefetch {
            PlainStorage::Prefetch(PlainPrefetchStorage::new(db, config.prefetch_offset, rx_op))
        } else {
            PlainStorage::Sync(PlainSyncStorage::new(db, rx_op))
        };
        let bench = Bench::new(config, tx_op);
        Self {
            bench,
            plain_storage,
        }
    }

    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        try_join!(
            self.plain_storage.run(cancel.clone()),
            self.bench.run(cancel)
        )?;
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
        tx_start: oneshot::Sender<()>,
    ) -> Self {
        let (tx_op, rx_op) = channel(1);

        let bench = Bench::new(bench_config, tx_op);
        let storage = Storage::new(
            storage_config,
            node_indices,
            db,
            node_table,
            addrs,
            network_id,
            rx_op,
            tx_start,
        );
        Self { bench, storage }
    }

    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        try_join!(self.storage.run(cancel.clone()), self.bench.run(cancel))?;
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
