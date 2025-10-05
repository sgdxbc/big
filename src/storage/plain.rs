use std::{fmt::Display, sync::Arc};

use bytes::Bytes;
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::info;

use crate::{db::DbWorker, latency::Latency};

use super::{StorageKey, StorageOp};

pub enum PlainStorage {
    Sync(PlainSyncStorage),
    Prefetch(PlainPrefetchStorage),
}

impl PlainStorage {
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        match self {
            Self::Sync(s) => s.run(cancel).await,
            Self::Prefetch(s) => s.run(cancel).await,
        }
    }
}

pub struct PlainSyncStorage {
    db: Arc<DB>,

    rx_op: Receiver<StorageOp>,
}

impl PlainSyncStorage {
    pub fn new(db: Arc<DB>, rx_op: Receiver<StorageOp>) -> Self {
        Self { db, rx_op }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let value = self.db.get(key.0)?;
                    let _ = tx_value.send(value.map(Into::into));
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
                StorageOp::VoteArchive(..) => unimplemented!(),
            }
        }
        Ok(())
    }
}

// this is pretty close to DBWorker + ActiveStateWorker + Storage::handle_op
// without sharding and network stuff
// maybe should be unified, but feels hard and overkill
pub struct PlainPrefetchStorage {
    db: Arc<DB>,

    rx_op: Receiver<StorageOp>,

    tasks: JoinSet<anyhow::Result<()>>,

    latencies: PlainPrefetchStorageLatencies,

    fetch: Option<PlainPrefetch>,
    tx_fetch: Sender<(StorageKey, oneshot::Sender<Option<Bytes>>)>,
}

#[derive(Default)]
struct PlainPrefetchStorageLatencies {
    fetch: Latency,
    bump: Latency,
}

impl Display for PlainPrefetchStorageLatencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "fetch: {}", self.fetch)?;
        write!(f, "bump: {}", self.bump)
    }
}

impl PlainPrefetchStorage {
    pub fn new(db: Arc<DB>, rx_op: Receiver<StorageOp>) -> Self {
        let (tx_fetch, rx_fetch) = channel(100);
        let fetch = PlainPrefetch::new(db.clone(), rx_fetch);
        Self {
            db,
            rx_op,
            tasks: JoinSet::new(),
            latencies: Default::default(),
            fetch: Some(fetch),
            tx_fetch,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let fetch = self.fetch.take().unwrap();
        let fetch = AbortOnDropHandle::new(tokio::spawn(fetch.run(cancel.clone())));

        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        self.tasks.shutdown().await;
        fetch.abort();

        info!("latencies:\n{}", self.latencies);
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
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
                        let record = self.latencies.fetch.record();
                        let _ = self.tx_fetch.send((storage_key, tx_value)).await;
                        record.stop()
                    }
                    StorageOp::Bump(items, tx_ok) => {
                        let record = self.latencies.bump.record();
                        let mut batch = WriteBatch::new();
                        for (key, value) in items {
                            match value {
                                Some(value) => batch.put(key.0, &value),
                                None => batch.delete(key.0),
                            }
                        }
                        if !batch.is_empty() {
                            self.db.write(batch)?
                        }
                        let _ = tx_ok.send(());
                        record.stop()
                    }
                    StorageOp::VoteArchive(..) => unimplemented!(),
                },
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }
}

struct PlainPrefetch {
    db: Arc<DB>,

    rx_op: Receiver<(StorageKey, oneshot::Sender<Option<Bytes>>)>,

    tasks: JoinSet<anyhow::Result<()>>,
}

impl PlainPrefetch {
    pub fn new(db: Arc<DB>, rx_op: Receiver<(StorageKey, oneshot::Sender<Option<Bytes>>)>) -> Self {
        Self {
            db,
            rx_op,
            tasks: JoinSet::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))?;
        self.tasks.shutdown().await;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<R> {
                Op((StorageKey, oneshot::Sender<Option<Bytes>>)),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(res) = self.tasks.join_next() => Event::TaskResult(res),
                else => break,
            } {
                Event::Op((storage_key, tx_value)) => {
                    let db = self.db.clone();
                    self.tasks.spawn_blocking(move || {
                        let value = db.get(storage_key.0)?;
                        let _ = tx_value.send(value.map(Into::into));
                        anyhow::Ok(())
                    });
                }
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }
}

impl PlainStorage {
    pub async fn prefill(
        // it is possible to take a Arc<DB>, but i explicitly want the DB to be closed (i.e.
        // dropped) after the prefilling, ensuring the benchmark will start cold
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
