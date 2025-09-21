use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, hash_map::Entry},
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
};
use tokio_util::{future::FutureExt, sync::CancellationToken};

use self::message::Message;

pub mod bench;
pub mod network;
pub mod plain;

pub type NodeIndex = u16;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct StorageKey([u8; 32]);

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

    pub fn shards_of(&self, node_indices: &[NodeIndex]) -> impl Iterator<Item = ShardIndex> {
        (0..self.num_stripe * self.num_shard_per_stripe()).filter(move |&shard_index| {
            self.nodes_of(shard_index)
                .any(|node_index| node_indices.contains(&node_index))
        })
    }
}

struct ShardWorker {
    index: ShardIndex,
    db: Arc<DB>,

    cancel: CancellationToken,
    // later extend the results of these two with proof and update info
    rx_op: Receiver<ShardWorkerOp>,
    tasks: JoinSet<anyhow::Result<()>>,
}

enum ShardWorkerOp {
    Get(Get, oneshot::Sender<GetRes>),
    Put(Put, oneshot::Sender<PutRes>),
}

// really unnecessary wrapper, just to keep the pattern consistent
struct Get(StorageKey);

struct GetRes {
    version: StateVersion,
    value: Option<Bytes>,
    // proof
}

struct Put {
    version: StateVersion,
    updates: Vec<(StorageKey, Option<Bytes>)>,
}

struct PutRes; // update info

impl ShardWorker {
    fn new(
        index: ShardIndex,
        db: Arc<DB>,
        cancel: CancellationToken,
        rx_op: Receiver<ShardWorkerOp>,
    ) -> Self {
        Self {
            index,
            db,
            cancel,
            rx_op,
            tasks: JoinSet::new(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        let prefix = self.index.to_le_bytes();
        let db_version_key = move || [&prefix[..], b".version"].concat();
        self.db.put(db_version_key(), b"0")?;
        loop {
            enum Event<R> {
                Op(ShardWorkerOp),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(result) = self.tasks.join_next() => Event::TaskResult(result),
                else => break,
            } {
                Event::Op(ShardWorkerOp::Get(Get(key), tx_res)) => {
                    let db = self.db.clone();
                    self.tasks.spawn(async move {
                        let snapshot = db.snapshot();
                        let Some(version) = snapshot.get_pinned(db_version_key())? else {
                            anyhow::bail!("missing version")
                        };
                        let value = snapshot.get([&prefix[..], &key.0].concat())?;
                        let res = GetRes {
                            version: str::from_utf8(&version)?.parse()?,
                            value: value.map(Bytes::from),
                        };
                        let _ = tx_res.send(res);
                        Ok(())
                    });
                }
                Event::Op(ShardWorkerOp::Put(put, tx_res)) => {
                    let mut batch = WriteBatch::new();
                    batch.put(db_version_key(), put.version.to_string());
                    for (key, value) in put.updates {
                        let db_key = [&prefix[..], &key.0].concat();
                        match value {
                            Some(v) => batch.put(&db_key, &v),
                            None => batch.delete(&db_key),
                        }
                    }
                    // perform write inline in the event loop, blocking the later `Get`s
                    // so that any Get received after this Put will see the updated version
                    self.db.write(batch)?;
                    let _ = tx_res.send(PutRes);
                }
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }
}

struct ActiveStateWorker {
    cancel: CancellationToken,
    rx_op: Receiver<ActiveStateOp>,

    purge_version: StateVersion,
    entries: HashMap<StorageKey, ActiveEntry>,
    entry_key_queue: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
    tx_fetch_values: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
}

enum ActiveStateOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    Install(StorageKey, ActiveEntry),
    Purge(StateVersion),
}

impl ActiveStateWorker {
    fn new(cancel: CancellationToken, rx_op: Receiver<ActiveStateOp>) -> Self {
        Self {
            cancel,
            rx_op,
            purge_version: 0,
            entries: Default::default(),
            entry_key_queue: Default::default(),
            tx_fetch_values: Default::default(),
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
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
                        let replaced = self.tx_fetch_values.insert(key, tx_value);
                        anyhow::ensure!(replaced.is_none(), "concurrent fetch for the same key");
                    }
                }
                ActiveStateOp::Install(key, entry) => {
                    if entry.version < self.purge_version {
                        continue;
                    }
                    if let Some(active_entry) = self.entries.get(&key)
                        && active_entry.version >= entry.version
                    {
                        continue;
                    }

                    if let Some(tx_value) = self.tx_fetch_values.remove(&key) {
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

    cancel: CancellationToken,
    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
    tx_message: Sender<(SendTo, Message)>,

    version: StateVersion,

    workers: Option<StorageWorkers>,
    tx_active_state_op: Sender<ActiveStateOp>,
    tx_shard_worker_ops: HashMap<ShardIndex, Sender<ShardWorkerOp>>,
}

struct StorageWorkers {
    active_state_worker: ActiveStateWorker,
    shard_workers: Vec<ShardWorker>,
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
        cancel: CancellationToken,
        rx_op: Receiver<StorageOp>,
        rx_message: Receiver<Message>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        let (tx_active_state_op, rx_active_state_op) = channel(100);
        let active_state_worker = ActiveStateWorker::new(cancel.clone(), rx_active_state_op);

        let mut shard_workers = Vec::new();
        let mut tx_shard_worker_ops = HashMap::new();
        for shard_index in config.shards_of(&node_indices) {
            let (tx_op, rx_op) = channel(100);
            let node = ShardWorker::new(shard_index, db.clone(), cancel.clone(), rx_op);
            shard_workers.push(node);
            tx_shard_worker_ops.insert(shard_index, tx_op);
        }

        Self {
            config,
            node_indices,
            cancel,
            rx_op,
            rx_message,
            tx_message,
            version: 0,
            workers: Some(StorageWorkers {
                active_state_worker,
                shard_workers,
            }),
            tx_active_state_op,
            tx_shard_worker_ops,
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let workers = self.workers.take().unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(workers.active_state_worker.run());
        for worker in workers.shard_workers {
            tasks.spawn(worker.run());
        }
        tasks.spawn(async move {
            self.cancel
                .clone()
                .run_until_cancelled(self.run_inner())
                .await
                .unwrap_or(Ok(()))
        });
        while let Some(result) = tasks.join_next().await {
            result??
        }
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
        match op {
            StorageOp::Fetch(storage_key, tx_value) => {
                let _ = self
                    .tx_active_state_op
                    .send(ActiveStateOp::Fetch(storage_key, tx_value))
                    .await;
            }
            StorageOp::Bump(updates, tx_ok) => {
                self.version += 1;
                let _ = self
                    .tx_active_state_op
                    .send(ActiveStateOp::Purge(
                        self.version - self.config.prefetch_offset,
                    ))
                    .await;
                let mut shard_updates = HashMap::new();
                for (storage_key, value) in updates {
                    let active_entry = ActiveEntry {
                        version: self.version,
                        value: value.clone(),
                    };
                    let _ = self
                        .tx_active_state_op
                        .send(ActiveStateOp::Install(storage_key, active_entry))
                        .await;
                    shard_updates
                        .entry(self.config.shard_of(&storage_key))
                        .or_insert_with(Vec::new)
                        .push((storage_key, value))
                }

                for (shard_index, updates) in shard_updates {
                    let Some(tx_shard_worker_op) = self.tx_shard_worker_ops.get(&shard_index)
                    else {
                        continue;
                    };
                    let put = Put {
                        version: self.version,
                        updates,
                    };
                    let (tx_res, rx_res) = oneshot::channel();
                    let _ = tx_shard_worker_op
                        .send(ShardWorkerOp::Put(put, tx_res))
                        .await;
                    // the underlying worker guarantees to not start Get before finishing
                    // earlier `Put`s, so its safe to handle Prefetch (and issue Get) before
                    // Put is done i.e. rx_res is resolved
                    spawn(
                        async move {
                            let Ok(PutRes) = rx_res.await else {
                                return;
                            };
                            // TODO broadcast update info
                        }
                        .with_cancellation_token_owned(self.cancel.clone()),
                    );
                }
                let _ = tx_ok.send(());
            }
            StorageOp::Prefetch(storage_key, tx_ok) => {
                let shard_index = self.config.shard_of(&storage_key);
                if let Some(tx_shard_worker_op) = self.tx_shard_worker_ops.get(&shard_index) {
                    let (tx_res, rx_res) = oneshot::channel();
                    let _ = tx_shard_worker_op
                        .send(ShardWorkerOp::Get(Get(storage_key), tx_res))
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
                let entry = ActiveEntry {
                    version: push_entry.version,
                    value: push_entry.value.map(Bytes::from),
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
            for (key, value) in items.by_ref() {
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
