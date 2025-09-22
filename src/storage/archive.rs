use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use bincode::config::standard;
use reed_solomon_simd::encode;
use rocksdb::{DB, DEFAULT_COLUMN_FAMILY_NAME};
use sha2::{Digest, Sha256};
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
    ArchiveRound, NodeIndex, SendTo, ShardIndex, StorageConfig, StripeIndex,
    message::{self, Message},
};

pub struct ArchiveWorker {
    config: StorageConfig,
    node_indices: Vec<NodeIndex>,
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
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        rx_op: Receiver<(ArchiveRound, oneshot::Sender<()>)>,
        rx_message: Receiver<message::PushShard>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        Self {
            config,
            node_indices,
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
            ArchiveKind::Unaligned => self.spawn_archive(self.round + 1, oneshot::channel().0), // ignored
            ArchiveKind::Aligned => {} // TODO vote
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
                    // not efficient, but probably never matters
                    self.reorder_messages.retain(|&r, _| r >= round);
                    self.spawn_archive(round, tx_snapshotted)
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
                    match self.config.archive_kind {
                        ArchiveKind::Disabled => unreachable!(),
                        ArchiveKind::Unaligned => {
                            self.spawn_archive(self.round + 1, oneshot::channel().0)
                        } // ignored
                        ArchiveKind::Aligned => {} // TODO vote
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_archive(&mut self, round: ArchiveRound, tx_snapshotted: oneshot::Sender<()>) {
        if self.tx_round_message.is_some() {
            warn!("archive interrupted");
            self.archive_task.abort()
        }

        let (tx_round_message, rx_round_message) = channel(100);
        let mut archive = Archive::new(
            self.config.clone(),
            self.node_indices.clone(),
            self.db.clone(),
            round,
            tx_snapshotted,
            rx_round_message,
            self.tx_message.clone(),
        );
        if let Some(messages) = self.reorder_messages.remove(&round) {
            for (shard_index, data) in messages {
                archive
                    .reorder_messages
                    .entry(self.config.stripe_of(shard_index))
                    .or_default()
                    .push((shard_index, data))
            }
        }
        self.archive_task = tokio::spawn(archive.run());
        self.tx_round_message = Some(tx_round_message);
        self.round = round
    }
}

struct Archive {
    config: StorageConfig,
    node_indices: Vec<NodeIndex>,
    db: Arc<DB>,

    tx_snapshotted: Option<oneshot::Sender<()>>,
    rx_message: Receiver<(ShardIndex, Vec<u8>)>,
    tx_message: Sender<(SendTo, Message)>,

    round: ArchiveRound,
    reorder_messages: HashMap<StripeIndex, Vec<(ShardIndex, Vec<u8>)>>,
}

impl Archive {
    fn new(
        config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        round: ArchiveRound,
        tx_snapshotted: oneshot::Sender<()>,
        rx_message: Receiver<(ShardIndex, Vec<u8>)>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        Self {
            config,
            node_indices,
            db,
            tx_snapshotted: Some(tx_snapshotted),
            rx_message,
            tx_message,
            round,
            reorder_messages: Default::default(),
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let snapshot = self.db.snapshot();
        let _ = self.tx_snapshotted.take().unwrap().send(());

        let hosting_shard_indices = self
            .config
            .hosted_by(&self.node_indices)
            .collect::<HashSet<_>>();
        'outer: for stripe_index in 0..self.config.num_stripe {
            let mut shards = BTreeMap::new();
            for shard_index in self.config.archived_as(stripe_index) {
                if !hosting_shard_indices.contains(&shard_index) {
                    continue;
                }
                let mut shard = Vec::new();
                let prefix = shard_index.to_le_bytes();
                let mut iter = snapshot.raw_iterator();
                iter.seek(prefix);
                iter.status()?;
                while let Some((key, value)) = iter.item() {
                    if !key.starts_with(&prefix) {
                        break;
                    }
                    shard.push((key[prefix.len()..].to_vec(), value.to_vec()));
                    iter.next();
                    iter.status()?
                }
                let shard = bincode::encode_to_vec(shard, standard())?;
                if self
                    .node_indices
                    .contains(&self.config.primary_node_of(shard_index))
                {
                    let push_shard = message::PushShard {
                        round: self.round,
                        shard_index,
                        data: shard.clone(),
                    };
                    let _ = self
                        .tx_message
                        .send((SendTo::All, Message::PushShard(push_shard)))
                        .await;
                }
                shards.insert(shard_index, shard);
            }

            if let Some(messages) = self.reorder_messages.remove(&stripe_index) {
                for (shard_index, data) in messages {
                    // TODO verify
                    shards.insert(shard_index, data);
                }
            }
            while shards.len() < self.config.num_shard_per_stripe() as usize {
                let Some((shard_index, shard)) = self.rx_message.recv().await else {
                    break 'outer;
                };
                let stripe_index2 = self.config.stripe_of(shard_index);
                if stripe_index2 < stripe_index {
                    warn!("stale shard received");
                    continue;
                }
                if stripe_index2 == stripe_index {
                    // TODO verify
                    shards.insert(shard_index, shard);
                } else {
                    self.reorder_messages
                        .entry(stripe_index2)
                        .or_default()
                        .push((shard_index, shard))
                }
            }

            let mut stripe = bincode::encode_to_vec(shards, standard())?;
            let k = self.config.num_shard_per_stripe() as usize;
            let stripe_len = stripe
                .len()
                // reed-solomon-simd requires even-byte sized shards
                .next_multiple_of(if k % 2 == 0 { k } else { k * 2 });
            stripe.resize(stripe_len, 0);
            let original_shards = stripe.chunks(stripe_len / k).collect::<Vec<_>>();
            let recovery_shards = encode(
                k,
                self.config.num_faulty_node as usize * 2,
                &original_shards,
            )?;
            for &node_index in &self.node_indices {
                let db_key = [
                    b".archive",
                    &self.round.to_le_bytes()[..],
                    &stripe_index.to_le_bytes()[..],
                    &node_index.to_le_bytes()[..],
                ]
                .concat();
                if (node_index as ShardIndex) < self.config.num_shard_per_stripe() {
                    self.db.put(db_key, original_shards[node_index as usize])?
                } else {
                    let recovery_index =
                        node_index - self.config.num_shard_per_stripe() as NodeIndex;
                    self.db
                        .put(db_key, &recovery_shards[recovery_index as usize])?
                }
            }
            let shard_digests = original_shards
                .into_iter()
                .chain(recovery_shards.iter().map(AsRef::as_ref))
                .map(|shard| Sha256::digest(shard).into())
                .collect::<Vec<[u8; 32]>>();
            self.db.put(
                [b".archive", &self.round.to_le_bytes()[..], b".digests"].concat(),
                bincode::encode_to_vec(&shard_digests, standard())?,
            )?;
        }
        let Some(cf) = self.db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME) else {
            unimplemented!()
        };
        self.db.delete_range_cf(
            cf,
            &b".archive"[..],
            &[b".archive", &self.round.to_le_bytes()[..]].concat(),
        )?;
        Ok(())
    }
}
