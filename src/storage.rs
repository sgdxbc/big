use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, hash_map::Entry},
    sync::Arc,
};

use bincode::{Decode, Encode, config::standard};
use bytes::Bytes;
use primitive_types::H256;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinSet,
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{NodeIndex, network::NetworkId};

use self::message::Message;

pub mod bench;
pub mod plain;

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

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<Option<Bytes>>),
    Bump(Vec<(StorageKey, Option<Bytes>)>, oneshot::Sender<()>),
    // TODO prefetch
}

type SegmentIndex = u32;

pub struct StorageConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,
    // for entry that is utilized at version `v`, push the `v - active_push_ahead` version to peers
    // at the same time, peers maintain the updated entries of the past `active_push_ahead` versions
    // locally
    // if the entry is not updated during `v - active_push_ahead..v`, the pushed version is the
    // entry at version `v`, otherwise, the version can be found from the locally maintained entries
    active_push_ahead: StateVersion,
}

impl StorageConfig {
    fn num_segment(&self) -> SegmentIndex {
        self.num_faulty_node as _
    }

    fn segment_of(&self, key: &StorageKey) -> SegmentIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_segment())
    }

    fn primary_node_of(&self, segment_index: SegmentIndex) -> NodeIndex {
        segment_index as _
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
    fetching_keys: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
    bumping: Option<oneshot::Sender<()>>,
    get_tasks: JoinSet<anyhow::Result<(StorageKey, Option<Vec<u8>>)>>,
    write_tasks: JoinSet<anyhow::Result<()>>,
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
    pub fn new(
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
            fetching_keys: Default::default(),
            bumping: None,
            get_tasks: JoinSet::new(),
            write_tasks: JoinSet::new(),
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<GR, PR> {
                Op(StorageOp),
                Message(Message),
                GetResult(GR),
                PutResult(PR),
            }
            match select! {
                Some(op) = self.rx_op.recv(), if self.bumping.is_none() => Event::Op(op),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                Some(result) = self.get_tasks.join_next() => Event::GetResult(result),
                Some(result) = self.write_tasks.join_next() => Event::PutResult(result),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await,
                Event::Message(message) => self.handle_message(message),
                Event::GetResult(result) => {
                    let (key, value) = result??;
                    self.handle_get_result(key, value)
                }
                Event::PutResult(result) => {
                    result??;
                    self.handle_write_result()
                }
            }
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageOp) {
        match op {
            StorageOp::Fetch(storage_key, tx_value) => self.handle_fetch(storage_key, tx_value),
            StorageOp::Bump(updates, tx_ok) => self.handle_bump(updates, tx_ok).await,
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::PushEntry(entry) => self.handle_push_entry(entry),
        }
    }

    fn handle_fetch(&mut self, key: StorageKey, tx_value: oneshot::Sender<Option<Bytes>>) {
        if let Some(entry) = self.active_state.get(&key) {
            assert!(entry.version + self.config.active_push_ahead >= self.version);
            let _ = tx_value.send(entry.value.clone());
            return;
            // in this branch we don't need to push entry anyway. if the entry is among active
            // state on our side, it must also be active on the other nodes
        }

        self.fetching_keys.insert(key, tx_value);
        let segment_index = self.config.segment_of(&key);
        if self
            .node_indices
            .contains(&self.config.primary_node_of(segment_index))
        {
            let db = self.db.clone();
            self.get_tasks
                .spawn_blocking(move || Ok((key, db.get(key.0)?)));
        }
    }

    async fn handle_bump(
        &mut self,
        updates: Vec<(StorageKey, Option<Bytes>)>,
        tx_ok: oneshot::Sender<()>,
    ) {
        if !self.fetching_keys.is_empty() {
            warn!("bump before fetch complete");
            self.fetching_keys.clear();
            self.get_tasks.shutdown().await
        }

        self.version += 1;
        while let Some(&Reverse((version, _))) = self.active_state_versions.peek()
            && version + self.config.active_push_ahead < self.version
        {
            let Reverse((_, key)) = self.active_state_versions.pop().unwrap();
            let Entry::Occupied(entry) = self.active_state.entry(key) else {
                unimplemented!()
            };
            if entry.get().version == version {
                entry.remove();
            }
        }

        let mut writes = Vec::new();
        for (key, value) in updates {
            let segment_index = self.config.segment_of(&key);
            if self
                .node_indices
                .contains(&self.config.primary_node_of(segment_index))
            {
                writes.push((key, value.clone()))
            }

            let entry = ActiveEntry {
                version: self.version,
                value,
            };

            self.active_state.insert(key, entry);
            self.active_state_versions
                .push(Reverse((self.version, key)))
        }

        if writes.is_empty() {
            let _ = tx_ok.send(());
        } else {
            self.bumping = Some(tx_ok);
            let db = self.db.clone();
            self.write_tasks.spawn_blocking(move || {
                let mut batch = WriteBatch::new();
                for (key, value) in writes {
                    match value {
                        Some(v) => batch.put(key.0, v),
                        None => batch.delete(key.0),
                    }
                }
                db.write(batch)?;
                Ok(())
            });
        }
    }

    fn handle_get_result(&mut self, key: StorageKey, value: Option<Vec<u8>>) {
        let entry = ActiveEntry {
            version: self.version,
            value: value.map(Bytes::from),
        };
        self.insert_entry(key, entry)
    }

    fn handle_write_result(&mut self) {
        if !self.write_tasks.is_empty() {
            return;
        }
        let Some(tx_ok) = self.bumping.take() else {
            unimplemented!()
        };
        let _ = tx_ok.send(());
    }

    fn handle_push_entry(&mut self, push_entry: message::PushEntry) {
        if push_entry.version < self.version - self.config.active_push_ahead {
            return;
        }
        if let Some(active_entry) = self.active_state.get(&push_entry.key)
            && active_entry.version >= push_entry.version
        {
            return;
        }

        let entry = ActiveEntry {
            version: push_entry.version,
            value: push_entry.value.map(Bytes::from),
        };
        self.insert_entry(push_entry.key, entry)
    }

    fn insert_entry(&mut self, key: StorageKey, entry: ActiveEntry) {
        if let Some(tx_value) = self.fetching_keys.remove(&key) {
            let _ = tx_value.send(entry.value.clone());

            let segment = self.config.segment_of(&key);
            if self
                .node_indices
                .contains(&self.config.primary_node_of(segment))
            {
                self.push_entry(key, entry.value.clone())
            }
        }

        self.active_state_versions
            .push(Reverse((entry.version, key)));
        self.active_state.insert(key, entry);
    }

    fn push_entry(&self, key: StorageKey, value: Option<Bytes>) {
        let push_entry = message::PushEntry {
            key,
            value: value.map(|v| v.to_vec()),
            version: self.version,
        };
        let _ = self
            .tx_message
            .try_send((SendTo::All, Message::PushEntry(push_entry)));
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

pub struct Incoming {
    cancel: CancellationToken,
    rx_bytes: Receiver<(NetworkId, Vec<u8>)>,
    tx_message: Sender<Message>,
}

impl Incoming {
    pub fn new(
        cancel: CancellationToken,
        rx_bytes: Receiver<(NetworkId, Vec<u8>)>,
        tx_message: Sender<Message>,
    ) -> Self {
        Self {
            cancel,
            rx_bytes,
            tx_message,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(Some((_, bytes))) =
            self.cancel.run_until_cancelled(self.rx_bytes.recv()).await
        {
            let (message, len) = bincode::decode_from_slice(&bytes, standard())?;
            anyhow::ensure!(len == bytes.len());
            let _ = self.tx_message.send(message).await;
        }
        Ok(())
    }
}

pub struct Outgoing {
    node_table: Vec<NetworkId>, // node index -> network id

    cancel: CancellationToken,
    rx_message: Receiver<(SendTo, Message)>,
    tx_bytes: Sender<(NetworkId, Bytes)>,
}

impl Outgoing {
    pub fn new(
        node_table: Vec<NetworkId>,
        cancel: CancellationToken,
        rx_message: Receiver<(SendTo, Message)>,
        tx_bytes: Sender<(NetworkId, Bytes)>,
    ) -> Self {
        Self {
            node_table,
            cancel,
            rx_message,
            tx_bytes,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(Some((send_to, message))) = self
            .cancel
            .run_until_cancelled(self.rx_message.recv())
            .await
        {
            let bytes = Bytes::from(bincode::encode_to_vec(&message, standard())?);
            match send_to {
                SendTo::All => {
                    for &network_id in &self.node_table {
                        let _ = self.tx_bytes.send((network_id, bytes.clone())).await;
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct Storage {
    core: StorageCore,
    incoming: Incoming,
    outgoing: Outgoing,
}

impl Storage {
    pub fn new(
        config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        node_table: Vec<NetworkId>,
        cancel: CancellationToken,
        rx_op: Receiver<StorageOp>,
        rx_incoming_bytes: Receiver<(NetworkId, Vec<u8>)>,
        tx_outgoing_bytes: Sender<(NetworkId, Bytes)>,
    ) -> Self {
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = channel(100);

        let core = StorageCore::new(
            config,
            node_indices,
            db,
            cancel.clone(),
            rx_op,
            rx_incoming_message,
            tx_outgoing_message,
        );
        let incoming = Incoming::new(cancel.clone(), rx_incoming_bytes, tx_incoming_message);
        let outgoing = Outgoing::new(node_table, cancel, rx_outgoing_message, tx_outgoing_bytes);
        Self {
            core,
            incoming,
            outgoing,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        try_join!(self.core.run(), self.incoming.run(), self.outgoing.run())?;
        Ok(())
    }
}
