use std::sync::Arc;

use bytes::Bytes;
use rocksdb::{DB, WriteBatch};
use tokio::{sync::mpsc::Receiver, task::JoinSet};
use tokio_util::sync::CancellationToken;

use super::{StorageKey, StorageOp};

pub struct PlainStorage {
    db: Arc<DB>,

    cancel: CancellationToken,
    rx_op: Receiver<StorageOp>,
}

impl PlainStorage {
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

    pub async fn prefill(
        db: impl Into<Arc<DB>>,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
    ) -> anyhow::Result<()> {
        let mut items = items.into_iter();
        let mut batch;
        let mut tasks = JoinSet::new();
        let db = db.into();
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
            if tasks.len() == std::thread::available_parallelism()?.get() {
                if let Some(res) = tasks.join_next().await {
                    res??
                }
            }
        }
        while let Some(res) = tasks.join_next().await {
            res??
        }
        Ok(())
    }
}
