use std::{collections::HashMap, sync::Arc};

use rocksdb::DB;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::storage::ArchiveKind;

use super::{
    ArchiveRound, SendTo, ShardIndex, StorageConfig,
    message::{self, Message},
};

pub struct ArchiveWorker {
    config: StorageConfig,
    db: Arc<DB>,

    rx_op: Receiver<(ArchiveRound, oneshot::Sender<()>)>,
    rx_message: Receiver<message::PushShard>,
    tx_message: Sender<(SendTo, Message)>,

    round: ArchiveRound,
    reorder_messages: HashMap<ArchiveRound, Vec<(ShardIndex, Vec<u8>)>>,

    archive_task: JoinHandle<anyhow::Result<()>>,
    tx_round_message: Option<Sender<(ShardIndex, Vec<u8>)>>,
}

impl ArchiveWorker {
    pub fn new(
        config: StorageConfig,
        db: Arc<DB>,
        rx_op: Receiver<(ArchiveRound, oneshot::Sender<()>)>,
        rx_message: Receiver<message::PushShard>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        Self {
            config,
            db,
            rx_op,
            rx_message,
            tx_message,
            round: 0,
            reorder_messages: Default::default(),
            archive_task: tokio::spawn(async { Ok(()) }),
            tx_round_message: None,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let result = cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()));
        if self.tx_round_message.is_some() {
            self.archive_task.abort()
        }
        result
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        match self.config.archive_kind {
            ArchiveKind::Disabled => return Ok(()),
            ArchiveKind::Unaligned => {
                self.round += 1;
                self.spawn_archive(oneshot::channel().0) // ignored
            }
            _ => {}
        }
        loop {
            enum Event<R> {
                Op((ArchiveRound, oneshot::Sender<()>)),
                Message(message::PushShard),
                TaskResult(R),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(msg) = self.rx_message.recv() => Event::Message(msg),
                result = &mut self.archive_task,
                    if self.tx_round_message.is_some() => Event::TaskResult(result),
                else => break,
            } {
                Event::Op((round, tx_snapshotted)) => {
                    anyhow::ensure!(matches!(self.config.archive_kind, ArchiveKind::Aligned));
                    self.round = round;
                    self.spawn_archive(tx_snapshotted)
                }
                Event::Message(push_shard) => {
                    if push_shard.round < self.round {
                        warn!("stale shard received");
                        continue;
                    }
                    if push_shard.round == self.round
                        && let Some(tx_round_message) = &self.tx_round_message
                    {
                        let _ = tx_round_message
                            .send((push_shard.shard_index, push_shard.data))
                            .await;
                    } else {
                        self.reorder_messages
                            .entry(push_shard.round)
                            .or_default()
                            .push((push_shard.shard_index, push_shard.data))
                    }
                }
                Event::TaskResult(result) => {
                    result??;
                    self.tx_round_message = None;
                    if matches!(self.config.archive_kind, ArchiveKind::Unaligned) {
                        self.round += 1;
                        self.spawn_archive(oneshot::channel().0) // ignored
                    } else {
                        assert!(matches!(self.config.archive_kind, ArchiveKind::Aligned));
                        // TODO vote
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_archive(&mut self, tx_snapshotted: oneshot::Sender<()>) {
        if self.tx_round_message.is_some() {
            warn!("archive interrupted");
            self.archive_task.abort()
        }

        let (tx_round_message, rx_round_message) = channel(100);
        let archive = Archive::new(
            self.config.clone(),
            self.db.clone(),
            self.round,
            tx_snapshotted,
            rx_round_message,
            self.tx_message.clone(),
        );
        // TODO insert reordered messages
        self.archive_task = tokio::spawn(archive.run());
        self.tx_round_message = Some(tx_round_message)
    }
}

struct Archive {
    config: StorageConfig,
    db: Arc<DB>,

    tx_snapshotted: Option<oneshot::Sender<()>>,
    rx_message: Receiver<(ShardIndex, Vec<u8>)>,
    tx_message: Sender<(SendTo, Message)>,

    round: ArchiveRound,
}

impl Archive {
    fn new(
        config: StorageConfig,
        db: Arc<DB>,
        round: ArchiveRound,
        tx_snapshotted: oneshot::Sender<()>,
        rx_message: Receiver<(ShardIndex, Vec<u8>)>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        Self {
            config,
            db,
            tx_snapshotted: Some(tx_snapshotted),
            rx_message,
            tx_message,
            round,
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let snapshot = self.db.snapshot();
        let _ = self.tx_snapshotted.take().unwrap().send(());
        Ok(())
    }
}
