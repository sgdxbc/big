use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, hash_map::Entry},
    fmt::Debug,
    iter,
    sync::Arc,
};

use bincode::{Decode, Encode};
use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinSet,
    try_join,
};
use tokio_util::{future::FutureExt, sync::CancellationToken};
use tracing::trace;

use self::{
    db::{DbWorker, DbWorkerOp, DbGet, DbPut, DbPutRes},
    message::Message,
};

pub mod bench;
pub mod db;
pub mod network;
pub mod plain;

pub type NodeIndex = u16;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct StorageKey([u8; 32]);

impl StorageKey {
    fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Debug for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageKey(0x{}...)", &self.to_hex()[..8])
    }
}

pub type StateVersion = u64;

type BumpUpdates = Vec<(StorageKey, Option<Bytes>)>;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    // technically Bump does not require a result channel; if back pressure is demanded, simply not
    // receiving the next op will do. the result channel is currently for measuring latency in the
    // benchmark
    Bump(BumpUpdates, oneshot::Sender<()>),
    // similar to Bump; only start measure latency after Prefetch is done
    Prefetch(StorageKey, oneshot::Sender<()>),
}

impl Debug for StorageOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageOp::Fetch(key, _) => write!(f, "Fetch({key:?})"),
            StorageOp::Bump(updates, _) => write!(f, "Bump(len={})", updates.len()),
            StorageOp::Prefetch(key, _) => write!(f, "Prefetch({key:?})"),
        }
    }
}

// this indexes _data_ shards exclusively. every shard holds a fraction of the
// `StorageKey`s. the _parity_ shards that no `StorageKey` maps to have no
// ShardIndex
pub type ShardIndex = u32;
type StripeIndex = u32;

#[derive(Clone)]
pub struct StorageConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,
    num_stripe: StripeIndex,
    num_backup: NodeIndex,
    // Prefetch that issued at version `v` matches a Fetch at version `v + prefetch_offset`
    // when primary nodes receive Prefetch, they push the prefetched entry to other nodes, so for a
    // Fetch at version `v`, a push of version `v - prefetch_offset` will be received on the nodes
    // the nodes should keep track of the updates for the last `prefetch_offset` versions, and they
    // can rely on the pushes for the even earlier updates
    prefetch_offset: StateVersion,
}

impl StorageConfig {
    fn num_shard_per_stripe(&self) -> ShardIndex {
        (self.num_faulty_node + 1) as _
    }

    fn shard_of(&self, key: &StorageKey) -> ShardIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_stripe * self.num_shard_per_stripe())
    }

    // fn stripe_of(&self, shard_index: ShardIndex) -> StripeIndex {
    //     shard_index / self.num_shard_per_stripe()
    // }

    fn primary_node_of(&self, shard_index: ShardIndex) -> NodeIndex {
        (shard_index % self.num_node as ShardIndex) as _
    }

    fn nodes_of(&self, shard_index: ShardIndex) -> impl Iterator<Item = NodeIndex> {
        let primary = self.primary_node_of(shard_index);
        let backup = (0..self.num_node - 1)
            .choose_multiple(
                &mut StdRng::seed_from_u64(shard_index as _),
                self.num_backup as _,
            )
            .into_iter()
            .map(move |index| index + (index >= primary) as NodeIndex);
        iter::once(primary).chain(backup)
    }

    fn shards_of(&self, node_indices: &[NodeIndex]) -> impl Iterator<Item = ShardIndex> {
        (0..self.num_stripe * self.num_shard_per_stripe()).filter(move |&shard_index| {
            self.nodes_of(shard_index)
                .any(|node_index| node_indices.contains(&node_index))
        })
    }
}

struct ActiveStateWorker {
    rx_op: Receiver<ActiveStateOp>,

    purge_version: StateVersion,
    entries: HashMap<StorageKey, ActiveEntry>,
    entry_key_queue: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
    fetch_tx_values: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
}

enum ActiveStateOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    Install(StorageKey, ActiveEntry),
    Purge(StateVersion),
}

impl ActiveStateWorker {
    fn new(rx_op: Receiver<ActiveStateOp>) -> Self {
        Self {
            rx_op,
            purge_version: 0,
            entries: Default::default(),
            entry_key_queue: Default::default(),
            fetch_tx_values: Default::default(),
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                ActiveStateOp::Fetch(key, tx_value) => {
                    if let Some(entry) = self.entries.get(&key) {
                        assert!(entry.version >= self.purge_version);
                        let _ = tx_value.send(entry.value.clone());
                    } else {
                        let replaced = self.fetch_tx_values.insert(key, tx_value);
                        anyhow::ensure!(replaced.is_none(), "concurrent fetch for the same key");
                    }
                }
                ActiveStateOp::Install(key, entry) => {
                    trace!(
                        "installing entry for key {:?} at version {}",
                        key, entry.version
                    );
                    if entry.version < self.purge_version {
                        continue;
                    }
                    if let Some(active_entry) = self.entries.get(&key)
                        && active_entry.version >= entry.version
                    {
                        continue;
                    }

                    if let Some(tx_value) = self.fetch_tx_values.remove(&key) {
                        let _ = tx_value.send(entry.value.clone());
                    }
                    self.entry_key_queue.push(Reverse((entry.version, key)));
                    self.entries.insert(key, entry);
                }
                ActiveStateOp::Purge(version) => {
                    self.purge_version = version;
                    while let Some(&Reverse((v, _))) = self.entry_key_queue.peek()
                        && v < self.purge_version
                    {
                        let Reverse((_, key)) = self.entry_key_queue.pop().unwrap();
                        let Entry::Occupied(entry) = self.entries.entry(key) else {
                            unreachable!()
                        };
                        if entry.get().version == v {
                            entry.remove();
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct Storage {
    config: StorageConfig,
    node_indices: Vec<NodeIndex>,
    shard_indices: HashSet<ShardIndex>,

    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
    tx_message: Sender<(SendTo, Message)>,

    version: StateVersion,
    cancel: CancellationToken,

    workers: Option<StorageWorkers>,
    tx_active_state_op: Sender<ActiveStateOp>,
    tx_db_worker_op: Sender<DbWorkerOp>,
}

struct StorageWorkers {
    active_state_worker: ActiveStateWorker,
    db_worker: DbWorker,
}

#[derive(Clone)]
struct ActiveEntry {
    version: StateVersion,
    value: Option<Bytes>,
}

pub enum SendTo {
    All,
    // individual?
}

impl Storage {
    fn new(
        config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        rx_op: Receiver<StorageOp>,
        rx_message: Receiver<Message>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        let (tx_active_state_op, rx_active_state_op) = channel(100);
        let active_state_worker = ActiveStateWorker::new(rx_active_state_op);

        let (tx_db_worker_op, rx_db_worker_op) = channel(100);
        let db_worker = DbWorker::new(config.clone(), db, rx_db_worker_op);

        Self {
            shard_indices: config.shards_of(&node_indices).collect(),
            config,
            node_indices,
            rx_op,
            rx_message,
            tx_message,
            version: 0,
            cancel: CancellationToken::new(),
            workers: Some(StorageWorkers {
                active_state_worker,
                db_worker,
            }),
            tx_active_state_op,
            tx_db_worker_op,
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let workers = self.workers.take().unwrap();
        let task = async {
            cancel
                .run_until_cancelled(self.run_inner())
                .await
                .unwrap_or(Ok(()))?;
            self.cancel.cancel();
            Ok(())
        };
        try_join!(
            task,
            workers.active_state_worker.run(cancel.clone()),
            workers.db_worker.run(cancel.clone()),
        )?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op(StorageOp),
                Message(Message),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(msg) = self.rx_message.recv() => Event::Message(msg),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await,
                Event::Message(message) => self.handle_message(message).await,
            }
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageOp) {
        trace!("{:?} version={} {op:?}", self.node_indices, self.version);
        match op {
            StorageOp::Fetch(storage_key, tx_value) => {
                let _ = self
                    .tx_active_state_op
                    .send(ActiveStateOp::Fetch(storage_key, tx_value))
                    .await;
            }
            StorageOp::Bump(mut updates, tx_ok) => {
                self.version += 1;
                if self.version >= self.config.prefetch_offset {
                    let _ = self
                        .tx_active_state_op
                        .send(ActiveStateOp::Purge(
                            self.version - self.config.prefetch_offset,
                        ))
                        .await;
                }
                for (storage_key, value) in &updates {
                    let active_entry = ActiveEntry {
                        version: self.version,
                        value: value.clone(),
                    };
                    let _ = self
                        .tx_active_state_op
                        .send(ActiveStateOp::Install(*storage_key, active_entry))
                        .await;
                }

                updates.retain(|(key, _)| self.shard_indices.contains(&self.config.shard_of(key)));
                let put = DbPut {
                    version: self.version,
                    updates,
                };
                let (tx_res, rx_res) = oneshot::channel();
                let _ = self
                    .tx_db_worker_op
                    .send(DbWorkerOp::Put(put, tx_res))
                    .await;
                // the underlying worker guarantees to not start Get before finishing
                // earlier `Put`s, so its safe to handle Prefetch (and issue Get) before
                // Put is done i.e. rx_res is resolved
                spawn(
                    async move {
                        let Ok(DbPutRes) = rx_res.await else {
                            return;
                        };
                        // TODO broadcast update info
                    }
                    .with_cancellation_token_owned(self.cancel.clone()),
                );
                let _ = tx_ok.send(());
            }
            StorageOp::Prefetch(storage_key, tx_ok) => {
                let shard_index = self.config.shard_of(&storage_key);
                if self
                    .shard_indices
                    .contains(&self.config.shard_of(&storage_key))
                {
                    let (tx_res, rx_res) = oneshot::channel();
                    let _ = self
                        .tx_db_worker_op
                        .send(DbWorkerOp::Get(DbGet(storage_key), tx_res))
                        .await;
                    let tx_active_state_op = self.tx_active_state_op.clone();
                    let tx_message = if self
                        .node_indices
                        .contains(&self.config.primary_node_of(shard_index))
                    {
                        Some(self.tx_message.clone())
                    } else {
                        None
                    };
                    spawn(
                        async move {
                            let Ok(get_res) = rx_res.await else {
                                return;
                            };
                            let entry = ActiveEntry {
                                version: get_res.version,
                                value: get_res.value.clone(),
                            };
                            let _ = tx_active_state_op
                                .send(ActiveStateOp::Install(storage_key, entry))
                                .await;
                            if let Some(tx_message) = tx_message {
                                let push_entry = message::PushEntry {
                                    key: storage_key,
                                    value: get_res.value.map(|v| v.to_vec()),
                                    version: get_res.version,
                                };
                                let _ = tx_message
                                    .send((SendTo::All, Message::PushEntry(push_entry)))
                                    .await;
                            }
                        }
                        .with_cancellation_token_owned(self.cancel.clone()),
                    );
                }
                let _ = tx_ok.send(());
            }
        }
    }

    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::PushEntry(push_entry) => {
                // TODO verify
                let entry = ActiveEntry {
                    version: push_entry.version,
                    value: push_entry.value.map(Into::into),
                };
                let _ = self
                    .tx_active_state_op
                    .send(ActiveStateOp::Install(push_entry.key, entry))
                    .await;
            }
        }
    }

    pub async fn prefill(
        db: DB,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
        config: &StorageConfig,
        node_indices: Vec<NodeIndex>,
    ) -> anyhow::Result<()> {
        let mut items = items.into_iter();
        let shard_indices = config.shards_of(&node_indices).collect::<HashSet<_>>();
        let mut batch;
        let mut tasks = JoinSet::new();
        let db = Arc::new(db);
        while {
            batch = WriteBatch::new();
            // wiki says "hundreds of keys"
            for (key, value) in &mut items {
                let shard_index = config.shard_of(&key);
                if shard_indices.contains(&shard_index) {
                    batch.put([&shard_index.to_le_bytes()[..], &key.0].concat(), &value);
                    if batch.len() == 1000 {
                        break;
                    }
                }
            }
            !batch.is_empty()
        } {
            let db = db.clone();
            tasks.spawn(async move { db.write(batch) });
            // not 100% cpu utilization, but more concurrency seems not improving
            if tasks.len() == std::thread::available_parallelism()?.get()
                && let Some(res) = tasks.join_next().await
            {
                res??
            }
        }
        while let Some(res) = tasks.join_next().await {
            res??
        }
        db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }
}

pub mod message {
    use bincode::{Decode, Encode};

    use crate::storage::{StateVersion, StorageKey};

    #[derive(Encode, Decode)]
    pub enum Message {
        PushEntry(PushEntry),
    }

    #[derive(Encode, Decode)]
    pub struct PushEntry {
        pub key: StorageKey,
        pub value: Option<Vec<u8>>,
        pub version: StateVersion,
    }
}

mod parse {
    use crate::parse::Extract;

    use super::StorageConfig;

    impl Extract for StorageConfig {
        fn extract(configs: &crate::parse::Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_node: configs.get("big.num-node")?,
                num_faulty_node: configs.get("big.num-faulty-node")?,
                num_stripe: configs.get("big.num-stripe")?,
                num_backup: configs.get("big.num-backup")?,
                prefetch_offset: configs.get("bench.prefetch-offset")?,
            })
        }
    }
}
