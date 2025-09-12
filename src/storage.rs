use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::{Arc, mpsc::Sender},
};

use bytes::Bytes;
use primitive_types::H256;
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
    task::JoinSet,
};
use tracing::warn;

pub mod bench;
pub mod plain;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

pub struct StorageConfig {
    // for entry that is utilized at version `v`, push the `v - active_push_ahead` version to peers
    // at the same time, peers maintain the updated entries of the past `active_push_ahead` versions
    // locally
    // if the entry is not updated during `v - active_push_ahead..v`, the pushed version is the
    // entry at version `v`, otherwise, the version can be found from the locally maintained entries
    active_push_ahead: StateVersion,
}

pub struct Storage {
    config: StorageConfig,
    db: Arc<DB>,

    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
    tx_message: Sender<(SendTo, Message)>,

    version: StateVersion,
    active_state: HashMap<StorageKey, ActiveEntry>,
    active_state_versions: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
    fetching_keys: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
    bumping: Option<oneshot::Sender<()>>,
    bumping_keys: HashMap<StorageKey, Option<Bytes>>,
    get_tasks: JoinSet<anyhow::Result<(StorageKey, Option<Vec<u8>>)>>,
    put_tasks: JoinSet<anyhow::Result<()>>,
}

#[derive(Clone)]
struct ActiveEntry {
    version: StateVersion,
    value: Option<Bytes>,
}

pub enum Message {
    PushEntry(message::PushEntry),
}

pub enum SendTo {
    All,
    // individual?
}

impl Storage {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<R> {
                Op(StorageOp),
                GetResult(R),
                Message(Message),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(result) = self.get_tasks.join_next() => Event::GetResult(result),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op),
                Event::GetResult(result) => {
                    let (key, value) = result??;
                    self.handle_get_result(key, value)
                }
                Event::Message(message) => self.handle_message(message),
            }
        }
        Ok(())
    }

    fn handle_op(&mut self, op: StorageOp) {
        match op {
            StorageOp::Fetch(storage_key, tx_value) => self.handle_fetch(storage_key, tx_value),
            StorageOp::Bump(updates, tx_ok) => self.handle_bump(updates, tx_ok),
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::PushEntry(entry) => self.handle_push_entry(entry),
        }
    }

    fn handle_fetch(&mut self, key: StorageKey, tx_value: oneshot::Sender<Option<Bytes>>) {
        if let Some(entry) = self.active_state.get(&key) {
            let _ = tx_value.send(entry.value.clone());
            return;
        }

        self.fetching_keys.insert(key, tx_value);
        let db = self.db.clone();
        self.get_tasks.spawn_blocking(move || {
            let value = db.get(key.0)?;
            Ok((key, value))
        });
    }

    fn handle_bump(
        &mut self,
        updates: Vec<(StorageKey, Option<Bytes>)>,
        tx_ok: oneshot::Sender<()>,
    ) {
        assert!(self.bumping_keys.is_empty());
        if !self.fetching_keys.is_empty() {
            warn!("bump while fetching not finished");
            self.fetching_keys.clear() // implicitly drop the result channels
        }

        self.version += 1;
        self.bumping = Some(tx_ok);

        for (key, value) in updates.clone() {
            if let Some(active_entry) = self.active_state.get(&key) {
                let updated_entry = self.update_entry(key, value, active_entry.clone());
                self.install_entry(key, updated_entry)
            } else {
                self.bumping_keys.insert(key, value);

                let db = self.db.clone();
                self.get_tasks.spawn_blocking(move || {
                    let value = db.get(key.0)?;
                    Ok((key, value))
                });
            }
        }

        let db = self.db.clone();
        self.put_tasks.spawn_blocking(move || {
            let mut batch = WriteBatch::new();
            for (key, value) in updates {
                match value {
                    Some(value) => batch.put(key.0, value),
                    None => batch.delete(key.0),
                }
            }
            db.write(batch)?;
            Ok(())
        });
    }

    fn handle_get_result(&mut self, key: StorageKey, value: Option<Vec<u8>>) {
        let entry = ActiveEntry {
            version: self.version,
            value: value.map(Bytes::from),
        };
        self.install_entry(key, entry)
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
        self.install_entry(push_entry.key, entry)
    }

    fn install_entry(&mut self, key: StorageKey, mut entry: ActiveEntry) {
        if let Some(tx_value) = self.fetching_keys.remove(&key) {
            let _ = tx_value.send(entry.value.clone());
        }

        if let Some(value) = self.bumping_keys.remove(&key) {
            entry = self.update_entry(key, value, entry)
        }

        self.active_state_versions
            .push(Reverse((entry.version, key)));
        self.active_state.insert(key, entry);
    }

    fn update_entry(
        &mut self,
        _key: StorageKey,
        value: Option<Bytes>,
        _entry: ActiveEntry,
    ) -> ActiveEntry {
        // TODO update root hash stuff
        ActiveEntry {
            version: self.version,
            value,
        }
    }
}

mod message {
    use crate::storage::{StateVersion, StorageKey};

    pub struct PushEntry {
        pub key: StorageKey,
        pub value: Option<Vec<u8>>,
        pub version: StateVersion,
    }
}
