use std::{
    fmt::Display,
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
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::info;

use crate::{latency::Latency, network::NetworkId, storage::plain::PlainPrefetchStorage};

use super::{
    BumpUpdates, NodeIndex, StateVersion, StorageConfig, StorageKey, StorageOp,
    network::Storage,
    plain::{PlainStorage, PlainSyncStorage},
};

#[derive(Clone)]
pub struct BenchConfig {
    num_key: u64,
    put_ratio: f64,
    prefetch_offset: StateVersion,
}

struct BenchProduce {
    config: BenchConfig,

    tx_command: Sender<Command>,
    tx_op: Sender<StorageOp>,

    rng: StdRng,
}

struct Command {
    start: Instant,
    task: Option<AbortOnDropHandle<anyhow::Result<Duration>>>,
    updates: BumpUpdates,
}

impl BenchProduce {
    fn new(config: BenchConfig, tx_command: Sender<Command>, tx_op: Sender<StorageOp>) -> Self {
        Self {
            config,
            tx_command,
            tx_op,
            rng: StdRng::seed_from_u64(117418),
        }
    }

    async fn run(mut self) {
        loop {
            enum Workload {
                Put(Bytes),
                Get,
            }
            let key = Bench::uniform_key(self.rng.random_range(0..self.config.num_key));
            let workload = if self.rng.random_bool(self.config.put_ratio) {
                let mut bytes = vec![0; 68];
                self.rng.fill(&mut bytes[..]);
                Workload::Put(bytes.into())
            } else {
                Workload::Get
            };
            let tx_op = self.tx_op.clone();
            let (task, updates) = match workload {
                Workload::Get => {
                    let task = spawn(async move {
                        let start = Instant::now();
                        let (tx_value, rx_value) = oneshot::channel();
                        let _ = tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                        rx_value.await?;
                        Ok(start.elapsed())
                    });
                    (Some(AbortOnDropHandle::new(task)), Default::default())
                }
                Workload::Put(value) => (None, vec![(key, Some(value))]),
            };
            let _ = self
                .tx_command
                .send(Command {
                    start: Instant::now(),
                    task,
                    updates,
                })
                .await;
        }
    }
}

pub struct Bench {
    tx_op: Sender<StorageOp>,

    latencies: BenchLatencies,

    produce: Option<BenchProduce>,
    rx_command: Receiver<Command>,
}

#[derive(Default)]
struct BenchLatencies {
    command: Latency,
    fetch_sync: Latency,
    fetch: Latency,
    bump: Latency,
}

impl Display for BenchLatencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "command: {}", self.command)?;
        writeln!(f, "fetch_sync: {}", self.fetch_sync)?;
        writeln!(f, "fetch: {}", self.fetch)?;
        write!(f, "bump: {}", self.bump)
    }
}

impl Bench {
    pub fn new(config: BenchConfig, tx_op: Sender<StorageOp>) -> Self {
        let (tx_command, rx_command) = channel((config.prefetch_offset + 1) as _);
        let generate = BenchProduce::new(config, tx_command, tx_op.clone());
        Self {
            tx_op,
            latencies: Default::default(),
            produce: Some(generate),
            rx_command,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let produce = self.produce.take().unwrap();
        let produce = AbortOnDropHandle::new(spawn(produce.run()));

        let start = Instant::now();
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;

        produce.abort();
        while let Some(command) = self.rx_command.recv().await {
            if let Some(task) = command.task {
                task.abort()
            }
        }

        let ops = self.latencies.command.len();
        let total = start.elapsed();
        let tps = ops as f64 / total.as_secs_f64();
        println!("ops: {ops}, total: {total:.1?}, tps: {tps:.2}");
        println!(
            "latencies:\n{}",
            self.latencies
                .to_string()
                .lines()
                .map(|l| format!("  {l}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        let mut interval_start = Instant::now();
        let mut interval_start_num_record = 0;
        while let Some(command) = self.rx_command.recv().await {
            if let Some(task) = command.task {
                let record = self.latencies.fetch_sync.record();
                let Ok(Ok(fetch_latency)) = task.await else {
                    break;
                };
                record.stop();
                self.latencies.fetch += fetch_latency
            }

            let record = self.latencies.bump.record();
            let (tx_ok, rx_ok) = oneshot::channel();
            let _ = self
                .tx_op
                .send(StorageOp::Bump(command.updates, tx_ok))
                .await;
            let Ok(()) = rx_ok.await else {
                break;
            };
            record.stop();
            self.latencies.command += command.start.elapsed();

            let now = Instant::now();
            let elapsed = now.duration_since(interval_start);
            if elapsed >= Duration::from_secs(1) {
                interval_start = now;
                let interval_num_record = self.latencies.command.len()
                    - replace(&mut interval_start_num_record, self.latencies.command.len());
                let tps = interval_num_record as f64 / elapsed.as_secs_f64();
                info!("interval tps: {tps:.2}")
            }
        }
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
            PlainStorage::Prefetch(PlainPrefetchStorage::new(db, rx_op))
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
        // try_join!(
        //     self.plain_storage.run(cancel.clone()),
        //     self.bench.run(cancel)
        // )?;
        let storage = spawn(self.plain_storage.run(cancel.clone()));
        let storage = async move {
            storage.await??;
            anyhow::Ok(())
        };
        let bench = spawn(self.bench.run(cancel));
        let bench = async move {
            bench.await??;
            anyhow::Ok(())
        };
        try_join!(storage, bench)?;

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
        let (tx_op, rx_op) = channel(100);

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
        // try_join!(self.storage.run(cancel.clone()), self.bench.run(cancel))?;
        let storage = spawn(self.storage.run(cancel.clone()));
        let storage = async move {
            storage.await??;
            anyhow::Ok(())
        };
        let bench = spawn(self.bench.run(cancel));
        let bench = async move {
            bench.await??;
            anyhow::Ok(())
        };
        try_join!(storage, bench)?;
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
