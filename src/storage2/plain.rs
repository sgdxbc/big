use std::sync::Arc;

use bytes::Bytes;
use rocksdb::DB;
use tokio::{
    sync::{
        mpsc::{Receiver, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

use crate::db::DbWorker;

use super::{StorageKey, StorageOp, StorageSchedOp, sched::StorageSched};

pub struct PlainStorage {
    sched: StorageSched,
    dispatch: Dispatch,
    db_worker: DbWorker,
}

struct Dispatch {
    rx_op: Receiver<StorageOp>,
    tx_get: flume::Sender<(StorageKey, oneshot::Sender<Option<Bytes>>)>,
    tx_write: flume::Sender<Vec<(StorageKey, Option<Bytes>)>>,
}

impl Dispatch {
    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(key, tx) => {
                    let _ = self.tx_get.send((key, tx));
                }
                StorageOp::Bump(updates) => {
                    let _ = self.tx_write.send(updates);
                }
            }
        }
        Ok(())
    }
}

impl PlainStorage {
    pub fn new(db: Arc<DB>, rx_sched_op: Receiver<StorageSchedOp>) -> Self {
        let (tx_op, rx_op) = channel(100);
        let (tx_bumped, rx_bumped) = channel(100);
        let (tx_get, rx_get) = flume::bounded(100);
        let (tx_write, rx_write) = flume::bounded(100);
        let sched = StorageSched::new(rx_sched_op, tx_op, rx_bumped);
        let dispatch = Dispatch {
            rx_op,
            tx_get,
            tx_write,
        };
        let db_worker = DbWorker::new(db, 50, rx_get, rx_write, tx_bumped);
        Self {
            sched,
            dispatch,
            db_worker,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let sched = self.sched.run(cancel.clone());
        let db_worker = self.db_worker.run(cancel.clone());
        let dispatch = async {
            cancel
                .run_until_cancelled(self.dispatch.run())
                .await
                .unwrap_or(Ok(()))
        };
        try_join!(sched, db_worker, dispatch)?;
        Ok(())
    }
}
