use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

use bytes::Bytes;
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use super::{StateVersion, StorageKey, StorageOp, StorageSchedOp};

pub struct StorageSched {
    rx_op: Receiver<StorageSchedOp>,
    tx_op: Sender<StorageOp>,
    rx_bumped: Receiver<StateVersion>,

    version: StateVersion, // according to the number of received Bump
    fetch_aheads: HashMap<StateVersion, Vec<FetchAheadState>>,
    bumped_version: StateVersion, // according to the sum of rx_bumped
    updates: HashMap<StorageKey, (StateVersion, Option<Bytes>)>,
    update_queue: BinaryHeap<Reverse<(StateVersion, StorageKey)>>,
}

struct FetchAheadState {
    key: StorageKey,
    tx: oneshot::Sender<Option<Bytes>>,
    rx: oneshot::Receiver<Option<Bytes>>,
}

impl StorageSched {
    pub fn new(
        rx_op: Receiver<StorageSchedOp>,
        tx_op: Sender<StorageOp>,
        rx_bumped: Receiver<StateVersion>,
    ) -> Self {
        Self {
            rx_op,
            tx_op,
            rx_bumped,
            version: 0,
            fetch_aheads: Default::default(),
            bumped_version: 0,
            updates: Default::default(),
            update_queue: Default::default(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op(StorageSchedOp),
                Bumped(StateVersion),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(v) = self.rx_bumped.recv() => Event::Bumped(v),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await,
                Event::Bumped(v) => {
                    self.bumped_version += v;
                    while self
                        .update_queue
                        .peek()
                        .is_some_and(|Reverse((ver, _))| *ver <= self.bumped_version)
                    {
                        let Reverse((ver, key)) = self.update_queue.pop().unwrap();
                        if self
                            .updates
                            .get(&key)
                            .is_some_and(|(cur_ver, _)| *cur_ver == ver)
                        {
                            self.updates.remove(&key);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageSchedOp) {
        match op {
            StorageSchedOp::FetchAhead(key, version_ahead, tx) => {
                let (tx_value, rx_value) = oneshot::channel();
                let _ = self.tx_op.send(StorageOp::Fetch(key, tx_value)).await;
                let state = FetchAheadState {
                    key,
                    tx,
                    rx: rx_value,
                };
                self.fetch_aheads
                    .entry(self.version + version_ahead)
                    .or_default()
                    .push(state);
                self.finalize_fetch()
            }
            StorageSchedOp::Bump(updates) => {
                self.version += 1;
                for (key, value) in &updates {
                    self.updates.insert(*key, (self.version, value.clone()));
                    self.update_queue.push(Reverse((self.version, *key)))
                }
                self.finalize_fetch();
                let _ = self.tx_op.send(StorageOp::Bump(updates)).await;
            }
        }
    }

    fn finalize_fetch(&mut self) {
        let Some(states) = self.fetch_aheads.remove(&self.version) else {
            return;
        };
        for state in states {
            if let Some((_, value)) = self.updates.get(&state.key) {
                let _ = state.tx.send(value.clone());
            } else {
                spawn(async move {
                    let _ = state.tx.send(state.rx.await?);
                    anyhow::Ok(())
                });
            }
        }
    }
}
