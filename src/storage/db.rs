use std::{collections::HashMap, sync::Arc};

use bincode::{Decode, Encode};
use bytes::Bytes;
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

use super::{ArchiveRound, ShardIndex, StateVersion, StorageConfig, StorageKey};

pub enum DbWorkerOp {
    Get(DbGet, oneshot::Sender<DbGetRes>),
    Put(DbPut, oneshot::Sender<DbPutRes>),
    Archive(ArchiveRound),
}

// really unnecessary wrapper, just to keep the pattern consistent
pub struct DbGet(pub StorageKey);

pub struct DbGetRes {
    pub version: StateVersion,
    pub value: Option<Bytes>,
    pub proof: Proof,
}

pub struct DbPut {
    pub version: StateVersion,
    pub updates: Vec<(StorageKey, Option<Bytes>)>,
}

pub struct DbPutRes(pub HashMap<ShardIndex, UpdateInfo>);

#[derive(Encode, Decode)]
pub struct Proof; // TODO
#[derive(Encode, Decode)]
pub struct UpdateInfo; // TODO

pub struct DbWorker {
    config: StorageConfig,
    db: Arc<DB>,

    rx_op: Receiver<DbWorkerOp>,
    tx_archive_op: Sender<(ArchiveRound, oneshot::Sender<()>)>,

    rx_snapshotted: Option<oneshot::Receiver<()>>,

    tasks: JoinSet<anyhow::Result<()>>,
}

impl DbWorker {
    pub fn new(
        config: StorageConfig,
        db: Arc<DB>,
        rx_op: Receiver<DbWorkerOp>,
        tx_archive_op: Sender<(ArchiveRound, oneshot::Sender<()>)>,
    ) -> Self {
        Self {
            config,
            db,
            rx_op,
            tx_archive_op,
            rx_snapshotted: None,
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
                    self.tasks.spawn_blocking(move || {
                        let snapshot = db.snapshot();
                        let Some(version) = snapshot.get(".version")? else {
                            anyhow::bail!("missing version")
                        };
                        let value =
                            snapshot.get([&shard_index.to_le_bytes()[..], &key.0].concat())?;
                        let res = DbGetRes {
                            version: str::from_utf8(&version)?.parse()?,
                            value: value.map(Into::into),
                            proof: Proof,
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

                    if let Some(rx_snapshotted) = self.rx_snapshotted.take() {
                        let _ = rx_snapshotted.await;
                    }

                    // perform write inline in the event loop, blocking the later `Get`s
                    // so that any Get received after this Put will see the updated version
                    self.db.write(batch)?;
                    let _ = tx_res.send(DbPutRes(Default::default()));
                }
                Event::Op(DbWorkerOp::Archive(round)) => {
                    let (tx_snapshotted, rx_snapshotted) = oneshot::channel();
                    let _ = self.tx_archive_op.send((round, tx_snapshotted)).await;
                    let replaced = self.rx_snapshotted.replace(rx_snapshotted);
                    assert!(replaced.is_none(), "concurrent archive requests")
                }
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }
}

pub struct VirtualDb;

#[allow(unused_variables)]
impl VirtualDb {
    pub fn verify(
        &self,
        version: StateVersion,
        key: &StorageKey,
        value: Option<&[u8]>,
        &Proof: &Proof,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn apply(
        &mut self,
        shard_index: ShardIndex,
        version: StateVersion,
        UpdateInfo: UpdateInfo,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
