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
                    let Some(cf) = self.db.cf_handle(&format!("{:02x}", key.0[0])) else {
                        anyhow::bail!("missing column family")
                    };
                    let value = self.db.get_cf(cf, key.0)?;
                    let _ = tx_value.send(value.map(Bytes::from));
                }
                StorageOp::Bump(updates, tx_ok) => {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        let Some(cf) = self.db.cf_handle(&format!("{:02x}", key.0[0])) else {
                            anyhow::bail!("missing column family")
                        };
                        match value {
                            Some(value) => batch.put_cf(cf, key.0, &value),
                            None => batch.delete_cf(cf, key.0),
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
        mut db: DB,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
    ) -> anyhow::Result<()> {
        let mut items = items.into_iter();
        for i in 0x00..=0xff {
            db.create_cf(format!("{i:02x}"), &Default::default())?
        }
        let mut batch;
        let mut tasks = JoinSet::new();
        let db = Arc::new(db);
        while {
            batch = WriteBatch::new();
            // wiki says "hundreds of keys"
            for (key, value) in items.by_ref().take(1000) {
                let Some(cf) = db.cf_handle(&format!("{:02x}", key.0[0])) else {
                    unimplemented!()
                };
                batch.put_cf(cf, key.0, &value)
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
        Ok(())
    }
}
