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

use super::{StorageKey, StorageOp, plain::Plain};

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
            let key = self.random_key();
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

    fn random_key(&mut self) -> StorageKey {
        StorageKey(StdRng::seed_from_u64(self.rng.random_range(0..=self.config.num_key)).random())
    }
}

pub struct BenchPlain {
    bench: Bench,
    plain: Plain,
}

impl BenchPlain {
    pub fn new(config: BenchConfig, db: Arc<DB>, cancel: CancellationToken) -> Self {
        let (tx_op, rx_op) = channel(1);
        let bench = Bench::new(config, cancel.clone(), tx_op);
        let plain = Plain::new(db, cancel, rx_op);
        Self { bench, plain }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        try_join!(self.plain.run(), self.bench.run())?;
        Ok(())
    }
}
