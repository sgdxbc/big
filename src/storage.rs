use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, hash_map::Entry},
    iter,
    sync::Arc,
};

use bincode::{Decode, Encode};
use bytes::Bytes;
use primitive_types::H256;
use rand::{Rng, SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::message::Message;

pub mod bench;
pub mod network;
pub mod plain;

pub type NodeIndex = u16;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct StorageKey([u8; 32]);

pub type StateVersion = u64;

impl StorageKey {
    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        Ok(Self(s.parse::<H256>()?.into()))
    }

    pub fn to_hex(&self) -> String {
        format!("{:x}", H256::from(self.0))
    }
}

type BumpUpdates = Vec<(StorageKey, Option<Bytes>)>;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    // technically Bump does not require a result channel; if back pressure is demanded, simply not
    // receiving the next op will do. the result channel is currently for measuring latency in the
    // benchmark
    Bump(BumpUpdates, oneshot::Sender<()>),

    Prefetch(StorageKey, oneshot::Sender<()>), // same reason as Bump
}

pub type SegmentIndex = u32;
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
    fn num_segment_per_stripe(&self) -> SegmentIndex {
        (self.num_faulty_node + 1) as _
    }

    fn segment_of(&self, key: &StorageKey) -> SegmentIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_stripe * self.num_segment_per_stripe())
    }

    fn stripe_of(&self, segment_index: SegmentIndex) -> StripeIndex {
        segment_index / self.num_segment_per_stripe()
    }

    // node index       0   1   2   3       stripe index
    // is primary of... 0   1               0
    //                      2   3           1
    //                          4   5       2
    //                  7           6       3
    //                  ...
    fn primary_node_of(&self, segment_index: SegmentIndex) -> NodeIndex {
        ((segment_index % self.num_segment_per_stripe() + self.stripe_of(segment_index))
            % self.num_node as SegmentIndex) as _
    }

    fn nodes_of(&self, segment_index: SegmentIndex) -> impl Iterator<Item = NodeIndex> {
        let primary = self.primary_node_of(segment_index);
        let backups = (0..self.num_node - 1).choose_multiple(
            &mut StdRng::seed_from_u64(segment_index as _),
            self.num_backup as _,
        );
        iter::once(primary).chain(
            backups
                .into_iter()
                .map(move |index| index + (index >= primary) as NodeIndex),
        )
    }

    pub fn segments_of(&self, node_indices: &[NodeIndex]) -> impl Iterator<Item = SegmentIndex> {
        (0..self.num_stripe * self.num_segment_per_stripe()).filter(move |&segment_index| {
            self.nodes_of(segment_index)
                .any(|node_index| node_indices.contains(&node_index))
        })
    }
}

pub struct StorageCore {
    config: StorageConfig,
    node_indices: Vec<NodeIndex>,
    db: Arc<DB>,

    cancel: CancellationToken,
    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
    tx_message: Sender<(SendTo, Message)>,

    version: StateVersion,
    active_state: HashMap<StorageKey, ActiveEntry>,
    active_state_versions: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
    fetch_tx_values: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
    bump_tx_ok: Option<oneshot::Sender<()>>,
    get_tasks: JoinSet<anyhow::Result<(StorageKey, ActiveEntry)>>,
    // write_tasks: JoinSet<anyhow::Result<()>>,
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

impl StorageCore {
    fn new(
        config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        cancel: CancellationToken,
        rx_op: Receiver<StorageOp>,
        rx_message: Receiver<Message>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        Self {
            config,
            node_indices,
            db,
            cancel,
            rx_op,
            rx_message,
            tx_message,
            version: 0,
            active_state: Default::default(),
            active_state_versions: Default::default(),
            fetch_tx_values: Default::default(),
            bump_tx_ok: None,
            get_tasks: JoinSet::new(),
            // write_tasks: JoinSet::new(),
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        while self.rx_op.recv().await.is_some() {}
        tracing::info!(?self.node_indices, "active state size: {}", self.active_state.len());
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            // enum Event<GR, WR> {
            enum Event<GR> {
                Op(StorageOp),
                Message(Message),
                GetResult(GR),
                // WriteResult(WR),
            }
            match select! {
                Some(op) = self.rx_op.recv(), if self.bump_tx_ok.is_none() => Event::Op(op),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                Some(result) = self.get_tasks.join_next() => Event::GetResult(result),
                // Some(result) = self.write_tasks.join_next() => Event::WriteResult(result),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await?,
                Event::Message(message) => self.handle_message(message),
                Event::GetResult(result) => {
                    let (key, entry) = result??;
                    self.handle_get_result(key, entry)
                } // Event::WriteResult(result) => {
                  //     result??;
                  //     self.handle_write_result()
                  // }
            }
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(storage_key, tx_value) => self.handle_fetch(storage_key, tx_value),
            StorageOp::Bump(updates, tx_ok) => self.handle_bump(updates, tx_ok),
            StorageOp::Prefetch(storage_key, tx_ok) => {
                self.handle_prefetch(storage_key, tx_ok).await
            }
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::PushEntry(entry) => self.handle_push_entry(entry),
        }
    }

    fn handle_fetch(
        &mut self,
        key: StorageKey,
        tx_value: oneshot::Sender<Option<Bytes>>,
    ) -> anyhow::Result<()> {
        if let Some(entry) = self.active_state.get(&key) {
            assert!(entry.version + self.config.prefetch_offset >= self.version);
            let _ = tx_value.send(entry.value.clone());
            return Ok(());
            // if the fetch is resolved in this way we don't need to push entry (even if we are the
            // primary). because if the entry is among active state on our side, it must also be
            // active on the other nodes, and they will resolve the fetch in the same way without
            // waiting for the push
        }

        let replaced = self.fetch_tx_values.insert(key, tx_value);
        anyhow::ensure!(replaced.is_none(), "concurrent fetch for the same key");
        Ok(())
    }

    async fn handle_prefetch(
        &mut self,
        key: StorageKey,
        tx_ok: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        let segment_index = self.config.segment_of(&key);
        if self
            .config
            .nodes_of(segment_index)
            .any(|node_index| self.node_indices.contains(&node_index))
        {
            // let db = self.db.clone();
            // let version = self.version;
            // let (tx_snapshot, rx_snapshot) = oneshot::channel();
            // self.get_tasks.spawn(async move {
            //     let Some(cf) = db.cf_handle(&format!("segment-{segment_index}")) else {
            //         anyhow::bail!("missing column family")
            //     };
            //     let snapshot = db.snapshot();
            //     let _ = tx_snapshot.send(());
            //     let value = snapshot.get_cf(cf, key.0)?;
            //     let entry = ActiveEntry {
            //         version,
            //         value: value.map(Bytes::from),
            //     };
            //     Ok((key, entry))
            // });
            // let _ = rx_snapshot.await;
            let Some(cf) = self.db.cf_handle(&format!("segment-{segment_index}")) else {
                anyhow::bail!("missing column family")
            };
            let value = self.db.get_cf(cf, key.0)?;
            let entry = ActiveEntry {
                version: self.version,
                value: value.map(Bytes::from),
            };
            self.handle_get_result(key, entry)
        }
        let _ = tx_ok.send(());
        Ok(())
    }

    fn handle_bump(
        &mut self,
        updates: Vec<(StorageKey, Option<Bytes>)>,
        tx_ok: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        if !self.fetch_tx_values.is_empty() {
            warn!("bump before fetch complete");
            self.fetch_tx_values.clear() // implicitly close result channels
        }

        self.version += 1;
        while let Some(&Reverse((version, _))) = self.active_state_versions.peek()
            && version + self.config.prefetch_offset < self.version
        {
            let Reverse((_, key)) = self.active_state_versions.pop().unwrap();
            let Entry::Occupied(entry) = self.active_state.entry(key) else {
                unreachable!()
            };
            if entry.get().version == version {
                entry.remove();
            }
        }

        let mut writes = Vec::new();
        for (key, value) in updates {
            let segment_index = self.config.segment_of(&key);
            if self
                .config
                .nodes_of(segment_index)
                .any(|node_index| self.node_indices.contains(&node_index))
            {
                writes.push((segment_index, key, value.clone()))
            }

            let entry = ActiveEntry {
                version: self.version,
                value,
            };
            self.may_install_entry(key, entry)
        }

        if writes.is_empty() {
            let _ = tx_ok.send(());
        } else {
            self.bump_tx_ok = Some(tx_ok);

            // let db = self.db.clone();
            // self.write_tasks.spawn_blocking(move || {
            //     let mut batch = WriteBatch::new();
            //     for (segment_index, key, value) in writes {
            //         let Some(cf) = db.cf_handle(&format!("segment-{segment_index}")) else {
            //             anyhow::bail!("missing column family")
            //         };
            //         match value {
            //             Some(v) => batch.put_cf(cf, key.0, v),
            //             None => batch.delete_cf(cf, key.0),
            //         }
            //     }
            //     db.write(batch)?;
            //     Ok(())
            // });

            let mut batch = WriteBatch::new();
            for (segment_index, key, value) in writes {
                let Some(cf) = self.db.cf_handle(&format!("segment-{segment_index}")) else {
                    anyhow::bail!("missing column family")
                };
                match value {
                    Some(v) => batch.put_cf(cf, key.0, v),
                    None => batch.delete_cf(cf, key.0),
                }
            }
            self.db.write(batch)?;
            self.handle_write_result()
        }
        Ok(())
    }

    fn handle_get_result(&mut self, key: StorageKey, entry: ActiveEntry) {
        self.may_push_entry(key, entry.value.clone());
        self.may_install_entry(key, entry)
    }

    fn handle_write_result(&mut self) {
        // if !self.write_tasks.is_empty() {
        //     return;
        // }
        let Some(tx_ok) = self.bump_tx_ok.take() else {
            unimplemented!()
        };
        let _ = tx_ok.send(());
    }

    fn handle_push_entry(&mut self, push_entry: message::PushEntry) {
        if push_entry.version + self.config.prefetch_offset < self.version {
            return;
        }

        let entry = ActiveEntry {
            version: push_entry.version,
            value: push_entry.value.map(Bytes::from),
        };
        self.may_install_entry(push_entry.key, entry)
    }

    fn may_install_entry(&mut self, key: StorageKey, entry: ActiveEntry) {
        if let Some(active_entry) = self.active_state.get(&key)
            && active_entry.version >= entry.version
        {
            return;
        }

        if let Some(tx_value) = self.fetch_tx_values.remove(&key) {
            let _ = tx_value.send(entry.value.clone());
        }

        self.active_state_versions
            .push(Reverse((entry.version, key)));
        self.active_state.insert(key, entry);
    }

    fn may_push_entry(&self, key: StorageKey, value: Option<Bytes>) {
        let segment = self.config.segment_of(&key);
        if !self
            .node_indices
            .contains(&self.config.primary_node_of(segment))
        {
            return;
        }

        let push_entry = message::PushEntry {
            key,
            value: value.map(|v| v.to_vec()),
            version: self.version,
        };
        let _ = self
            .tx_message
            .try_send((SendTo::All, Message::PushEntry(push_entry)));
    }

    pub fn prefill(
        db: &mut DB,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
        config: &StorageConfig,
        node_indices: Vec<NodeIndex>,
    ) -> anyhow::Result<()> {
        let segment_indices = config.segments_of(&node_indices).collect::<HashSet<_>>();
        for segment_index in &segment_indices {
            db.create_cf(format!("segment-{segment_index}"), &Default::default())?
        }

        let mut items = items.into_iter();
        let mut batch;
        while {
            batch = WriteBatch::new();
            // wiki says "hundreds of keys"
            for (key, value) in items.by_ref() {
                let segment_index = config.segment_of(&key);
                if segment_indices.contains(&segment_index) {
                    let Some(cf) = db.cf_handle(&format!("segment-{segment_index}")) else {
                        unimplemented!()
                    };
                    batch.put_cf(cf, key.0, &value);
                    if batch.len() == 1000 {
                        break;
                    }
                }
            }
            !batch.is_empty()
        } {
            db.write(batch)?
        }
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
