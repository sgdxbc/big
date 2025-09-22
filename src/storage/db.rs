use std::sync::Arc;

use bytes::Bytes;
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use super::{StateVersion, StorageConfig, StorageKey};

pub struct DbWorker {
    config: StorageConfig,
    db: Arc<DB>,

    // later extend the results of these two with proof and update info
    rx_op: Receiver<DbWorkerOp>,
    tasks: JoinSet<anyhow::Result<()>>,
}

pub enum DbWorkerOp {
    Get(DbGet, oneshot::Sender<DbGetRes>),
    Put(DbPut, oneshot::Sender<DbPutRes>),
}

// really unnecessary wrapper, just to keep the pattern consistent
pub struct DbGet(pub StorageKey);

pub struct DbGetRes {
    pub version: StateVersion,
    pub value: Option<Bytes>,
    // proof
}

pub struct DbPut {
    pub version: StateVersion,
    pub updates: Vec<(StorageKey, Option<Bytes>)>,
}

pub struct DbPutRes; // update info

impl DbWorker {
    pub fn new(config: StorageConfig, db: Arc<DB>, rx_op: Receiver<DbWorkerOp>) -> Self {
        Self {
            config,
            db,
            rx_op,
            tasks: JoinSet::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        self.db.put(".version", 0.to_string())?;
        loop {
            enum Event<R> {
                Op(DbWorkerOp),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(result) = self.tasks.join_next() => Event::TaskResult(result),
                else => break,
            } {
                Event::Op(DbWorkerOp::Get(DbGet(key), tx_res)) => {
                    let shard_index = self.config.shard_of(&key);
                    let db = self.db.clone();
                    self.tasks.spawn(async move {
                        let snapshot = db.snapshot();
                        let Some(version) = snapshot.get_pinned(".version")? else {
                            anyhow::bail!("missing version")
                        };
                        let value =
                            snapshot.get([&shard_index.to_le_bytes()[..], &key.0].concat())?;
                        let res = DbGetRes {
                            version: str::from_utf8(&version)?.parse()?,
                            value: value.map(Into::into),
                        };
                        let _ = tx_res.send(res);
                        Ok(())
                    });
                }
                Event::Op(DbWorkerOp::Put(put, tx_res)) => {
                    let mut batch = WriteBatch::new();
                    batch.put(".version", put.version.to_string());
                    for (key, value) in put.updates {
                        let shard_index = self.config.shard_of(&key);
                        let db_key = [&shard_index.to_le_bytes()[..], &key.0].concat();
                        match value {
                            Some(v) => batch.put(&db_key, &v),
                            None => batch.delete(&db_key),
                        }
                    }
                    // perform write inline in the event loop, blocking the later `Get`s
                    // so that any Get received after this Put will see the updated version
                    self.db.write(batch)?;
                    let _ = tx_res.send(DbPutRes);
                }
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }
}
