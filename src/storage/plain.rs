use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, hash_map::Entry},
    sync::Arc,
};

use bytes::Bytes;
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use super::{StateVersion, StorageKey, StorageOp};

pub enum PlainStorage {
    Sync(PlainSyncStorage),
    Prefetch(PlainPrefetchStorage),
}

impl PlainStorage {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Sync(mut s) => s.run().await,
            Self::Prefetch(mut s) => s.run().await,
        }
    }
}

pub struct PlainSyncStorage {
    db: Arc<DB>,

    cancel: CancellationToken,
    rx_op: Receiver<StorageOp>,
}

impl PlainSyncStorage {
    pub fn new(db: Arc<DB>, cancel: CancellationToken, rx_op: Receiver<StorageOp>) -> Self {
        Self { db, cancel, rx_op }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        while self.rx_op.recv().await.is_some() {}
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let value = self.db.get(key.0)?;
                    let _ = tx_value.send(value.map(Bytes::from));
                }
                StorageOp::Bump(updates, tx_ok) => {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        match value {
                            Some(value) => batch.put(key.0, &value),
                            None => batch.delete(key.0),
                        }
                    }
                    self.db.write(batch)?;
                    let _ = tx_ok.send(());
                }
                StorageOp::Prefetch(_, tx_ok) => {
                    let _ = tx_ok.send(());
                }
            }
        }
        Ok(())
    }
}

pub struct PlainPrefetchStorage {
    db: Arc<DB>,
    prefetch_offset: StateVersion,

    cancel: CancellationToken,
    rx_op: Receiver<StorageOp>,

    version: StateVersion,
    active_entries: HashMap<StorageKey, ActiveEntry>,
    active_versioned_keys: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
    fetch_tx_values: HashMap<StorageKey, oneshot::Sender<Option<Bytes>>>,
    tasks: JoinSet<anyhow::Result<(StorageKey, ActiveEntry)>>,
}

struct ActiveEntry {
    version: StateVersion,
    value: Option<Bytes>,
}

impl PlainPrefetchStorage {
    pub fn new(
        db: Arc<DB>,
        prefetch_offset: StateVersion,
        cancel: CancellationToken,
        rx_op: Receiver<StorageOp>,
    ) -> Self {
        Self {
            db,
            prefetch_offset,
            cancel,
            rx_op,
            version: 0,
            active_entries: Default::default(),
            active_versioned_keys: Default::default(),
            fetch_tx_values: Default::default(),
            tasks: JoinSet::new(),
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        while self.rx_op.recv().await.is_some() {}
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        self.db.put(b".version", 0u64.to_le_bytes())?;
        loop {
            enum Event<R> {
                Op(StorageOp),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(res) = self.tasks.join_next() => Event::TaskResult(res),
                else => break,
            } {
                Event::Op(op) => match op {
                    StorageOp::Fetch(storage_key, tx_value) => {
                        if let Some(entry) = self.active_entries.get(&storage_key) {
                            assert!(entry.version + self.prefetch_offset >= self.version);
                            let _ = tx_value.send(entry.value.clone());
                            continue;
                        }
                        let replaced = self.fetch_tx_values.insert(storage_key, tx_value);
                        anyhow::ensure!(replaced.is_none(), "duplicate fetch for the same key")
                    }
                    StorageOp::Bump(items, tx_ok) => {
                        self.version += 1;
                        while let Some(&Reverse((version, _))) = self.active_versioned_keys.peek()
                            && version + self.prefetch_offset < self.version
                        {
                            let Reverse((_, key)) = self.active_versioned_keys.pop().unwrap();
                            let Entry::Occupied(entry) = self.active_entries.entry(key) else {
                                unreachable!()
                            };
                            if entry.get().version == version {
                                entry.remove();
                            }
                        }
                        let mut batch = WriteBatch::new();
                        batch.put(b".version", self.version.to_le_bytes());
                        for (key, value) in items {
                            let entry = ActiveEntry {
                                version: self.version,
                                value: value.clone(),
                            };
                            self.active_entries.insert(key, entry);
                            self.active_versioned_keys
                                .push(Reverse((self.version, key)));
                            match value {
                                Some(value) => batch.put(key.0, &value),
                                None => batch.delete(key.0),
                            }
                        }
                        self.db.write(batch)?;
                        let _ = tx_ok.send(());
                    }
                    StorageOp::Prefetch(storage_key, tx_ok) => {
                        let db = self.db.clone();
                        self.tasks.spawn(async move {
                            let snapshot = db.snapshot();
                            let Some(version) = snapshot.get(b".version")? else {
                                unimplemented!("no version found")
                            };
                            let version = StateVersion::from_le_bytes(
                                version.try_into().expect("valid version"),
                            );
                            let value = snapshot.get(storage_key.0)?;
                            let active_entry = ActiveEntry {
                                version,
                                value: value.map(Bytes::from),
                            };
                            Ok((storage_key, active_entry))
                        });
                        let _ = tx_ok.send(());
                    }
                },
                Event::TaskResult(result) => {
                    let (storage_key, active_entry) = result??;
                    let entry = self.active_entries.entry(storage_key);
                    if let Entry::Occupied(entry) = &entry
                        && entry.get().version >= active_entry.version
                    {
                        continue;
                    };
                    if let Some(tx_value) = self.fetch_tx_values.remove(&storage_key) {
                        let _ = tx_value.send(active_entry.value.clone());
                    }
                    self.active_versioned_keys
                        .push(Reverse((active_entry.version, storage_key)));
                    entry.insert_entry(active_entry);
                }
            }
        }
        Ok(())
    }
}

impl PlainStorage {
    pub async fn prefill(
        db: DB,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
    ) -> anyhow::Result<()> {
        let mut items = items.into_iter();
        let mut batch;
        let mut tasks = JoinSet::new();
        let db = Arc::new(db);
        while {
            batch = WriteBatch::new();
            // wiki says "hundreds of keys"
            for (key, value) in items.by_ref().take(1000) {
                batch.put(key.0, &value)
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
