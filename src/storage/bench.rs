use std::{
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

use crate::network::{Mesh, Network, NetworkId};

use super::{NodeIndex, Storage, StorageConfig, StorageKey, StorageOp, plain::PlainStorage};

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
        loop {
            let start;
            let key = Self::uniform_key(self.rng.random_range(0..=self.config.num_key));
            if self.rng.random_bool(self.config.put_ratio) {
                let mut value = vec![0; 68];
                self.rng.fill(&mut value[..]);
                let updates = vec![(key, Some(value.into()))];

                start = Instant::now();
                let (tx_ok, rx_ok) = oneshot::channel();
                let _ = self.tx_op.send(StorageOp::Bump(updates, tx_ok)).await;
                let _ = rx_ok.await?;
            } else {
                start = Instant::now();
                let (tx_value, rx_value) = oneshot::channel();
                let _ = self.tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                let _ = rx_value.await?;
                let (tx_ok, rx_ok) = oneshot::channel();
                let _ = self
                    .tx_op
                    .send(StorageOp::Bump(Default::default(), tx_ok))
                    .await;
                let _ = rx_ok.await?;
            }
            let duration = start.elapsed();
            self.records.push((start, duration))
        }
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
    pub fn new(config: BenchConfig, db: Arc<DB>, cancel: CancellationToken) -> Self {
        let (tx_op, rx_op) = channel(1);
        let bench = Bench::new(config, cancel.clone(), tx_op);
        let plain_storage = PlainStorage::new(db, cancel, rx_op);
        Self {
            bench,
            plain_storage,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        try_join!(self.plain_storage.run(), self.bench.run())?;
        Ok(())
    }
}

pub struct BenchStorage {
    bench: Bench,
    storage: Storage,
    network: Network,
    mesh: Mesh,
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
    ) -> Self {
        let (tx_incoming_connection, rx_incoming_connection) = channel(100);
        let (tx_outgoing_connection, rx_outgoing_connection) = channel(100);
        let (tx_incoming_bytes, rx_incoming_bytes) = channel(100);
        let (tx_outgoing_bytes, rx_outgoing_bytes) = channel(100);
        let (tx_op, rx_op) = channel(1);

        let network = Network::new(
            network_id,
            cancel.clone(),
            rx_incoming_connection,
            rx_outgoing_connection,
            rx_outgoing_bytes,
            tx_incoming_bytes,
        );
        let mesh = Mesh::new(
            addrs,
            network_id,
            tx_outgoing_connection,
            tx_incoming_connection,
        );
        let bench = Bench::new(bench_config, cancel.clone(), tx_op);
        let storage = Storage::new(
            storage_config,
            node_indices,
            db,
            node_table,
            cancel.clone(),
            rx_op,
            rx_incoming_bytes,
            tx_outgoing_bytes,
        );
        Self {
            bench,
            storage,
            network,
            mesh,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let task = async {
            self.mesh.run().await?;
            try_join!(self.bench.run(), self.storage.run())?;
            Ok(())
        };
        try_join!(self.network.run(), task)?;
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
