use std::{
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

use crate::NodeIndex;

use super::{StorageCore, StorageConfig, StorageKey, StorageOp, plain::PlainStorage};

pub struct BenchConfig {
    num_key: u64,
    put_ratio: f64,
}

pub struct Bench {
    config: BenchConfig,

    cancel: CancellationToken,
    tx_op: Sender<StorageOp>,

    rng: StdRng,
    records: Vec<(Instant, Duration)>,
}

impl Bench {
    pub fn new(config: BenchConfig, cancel: CancellationToken, tx_op: Sender<StorageOp>) -> Self {
        Self {
            config,
            cancel,
            tx_op,
            rng: StdRng::seed_from_u64(117418),
            records: Default::default(),
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        if let (Some(first), Some(last)) = (self.records.first(), self.records.last()) {
            let total = last.0.duration_since(first.0) + last.1;
            let ops = self.records.len() as f64;
            let tps = ops / total.as_secs_f64();
            println!("ops: {}, total: {:.1?}, tps: {:.2}", ops, total, tps);
            let mean = self.records.iter().map(|r| r.1).sum::<Duration>() / ops as u32;
            println!("mean latency: {:?}", mean)
        }
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        let mut value = vec![0; 68];
        loop {
            let start;
            let key = Self::uniform_key(self.rng.random_range(0..=self.config.num_key));
            if self.rng.random_bool(self.config.put_ratio) {
                self.rng.fill(&mut value[..]);
                let (tx_ok, rx_ok) = oneshot::channel();
                start = Instant::now();
                let _ = self
                    .tx_op
                    .send(StorageOp::Bump(
                        vec![(key, Some(Bytes::copy_from_slice(&value)))],
                        tx_ok,
                    ))
                    .await;
                let _ = rx_ok.await;
            } else {
                let (tx_value, rx_value) = oneshot::channel();
                start = Instant::now();
                let _ = self.tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                let _ = rx_value.await;
            }
            let duration = start.elapsed();
            self.records.push((start, duration))
        }
    }

    fn uniform_key(index: u64) -> StorageKey {
        StorageKey(StdRng::seed_from_u64(index).random())
    }

    pub fn prefill_items(
        config: BenchConfig,
        mut rng: StdRng,
    ) -> impl Iterator<Item = (StorageKey, Bytes)> {
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
    pub fn new(config: BenchConfig, db: Arc<DB>, cancel: CancellationToken) -> Self {
        let (tx_op, rx_op) = channel(1);
        let bench = Bench::new(config, cancel.clone(), tx_op);
        let plain = PlainStorage::new(db, cancel, rx_op);
        Self {
            bench,
            plain_storage: plain,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        try_join!(self.plain_storage.run(), self.bench.run())?;
        Ok(())
    }
}

pub struct BenchStorage {
    bench: Bench,
    storage: StorageCore,
}

impl BenchStorage {
    pub fn new(
        bench_config: BenchConfig,
        storage_config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        cancel: CancellationToken,
    ) -> Self {
        let (tx_op, rx_op) = channel(1);
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = channel(100);

        let bench = Bench::new(bench_config, cancel.clone(), tx_op);
        let storage = StorageCore::new(
            storage_config,
            node_indices,
            db,
            cancel.clone(),
            rx_op,
            rx_incoming_message,
            tx_outgoing_message,
        );
        Self { bench, storage }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
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
            })
        }
    }
}
