use std::sync::Arc;

use rocksdb::DB;
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::ArchiveRound;

pub enum ArchiveWorker {
    Disabled,
    Unaligned(UnalignedArchiveWorker),
    Aligned(AlignedArchiveWorker),
}

impl ArchiveWorker {
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        match self {
            ArchiveWorker::Disabled => Ok(()),
            Self::Unaligned(w) => w.run(cancel).await,
            Self::Aligned(w) => w.run(cancel).await,
        }
    }
}

pub struct UnalignedArchiveWorker {
    db: Arc<DB>,

    round: ArchiveRound,
}

impl UnalignedArchiveWorker {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db, round: 0 }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            self.round += 1;
            let archive = Archive::new(
                self.db.clone(),
                self.round,
                oneshot::channel().0, // ignored
            );
            archive.run().await?
        }
    }
}

pub struct AlignedArchiveWorker {
    db: Arc<DB>,

    rx_op: Receiver<(ArchiveRound, oneshot::Sender<()>)>,

    archiving: bool,

    archive_task: JoinHandle<anyhow::Result<()>>,
}

impl AlignedArchiveWorker {
    pub fn new(db: Arc<DB>, rx_op: Receiver<(ArchiveRound, oneshot::Sender<()>)>) -> Self {
        Self {
            db,
            rx_op,
            archiving: false,
            archive_task: tokio::spawn(async { Ok(()) }),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let result = cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()));
        if self.archiving {
            self.archive_task.abort()
        }
        result
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<R> {
                Op((ArchiveRound, oneshot::Sender<()>)),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                result = &mut self.archive_task, if self.archiving => Event::TaskResult(result),
            } {
                Event::Op((round, tx_snapshotted)) => {
                    if self.archiving {
                        warn!("archive interrupted");
                        self.archive_task.abort()
                    }
                    let archive = Archive::new(self.db.clone(), round, tx_snapshotted);
                    self.archive_task = tokio::spawn(archive.run())
                }
                Event::TaskResult(result) => {
                    result??;
                    self.archiving = false;
                    // TODO
                }
            }
        }
    }
}

struct Archive {
    db: Arc<DB>,

    tx_snapshotted: Option<oneshot::Sender<()>>,

    round: ArchiveRound,
}

impl Archive {
    fn new(db: Arc<DB>, round: ArchiveRound, tx_snapshotted: oneshot::Sender<()>) -> Self {
        Self {
            db,
            tx_snapshotted: Some(tx_snapshotted),
            round,
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let snapshot = self.db.snapshot();
        let _ = self.tx_snapshotted.take().unwrap().send(());
        Ok(())
    }
}
