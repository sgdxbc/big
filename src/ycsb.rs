use bincode::{Decode, Encode};
use bytes::Bytes;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

use crate::storage2::{BumpUpdates, StorageKey, StorageOp};

#[derive(Debug, Encode, Decode)]
pub enum YcsbOp {
    Read(String),
    Update(String, Vec<u8>),
}

#[derive(Debug, Encode, Decode)]
pub enum YcsbRes {
    Read(Option<Vec<u8>>),
    Update,
}

pub struct Ycsb {
    front: YcsbFront,
    execute: YcsbExecute,
}

enum InflightData {
    Read(oneshot::Receiver<Option<Bytes>>),
    Update(String, Vec<u8>),
}

struct YcsbExecute {
    rx_inflight: Receiver<(oneshot::Sender<YcsbRes>, InflightData)>,
    tx_bump: Sender<BumpUpdates>,
}

impl YcsbExecute {
    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some((tx, data)) = self.rx_inflight.recv().await {
            let (updates, res) = match data {
                InflightData::Read(rx) => {
                    let value = rx.await?;
                    (Default::default(), YcsbRes::Read(value.map(Into::into)))
                }
                InflightData::Update(key, value) => (
                    vec![(StorageKey::digest(key), Some(value.into()))],
                    YcsbRes::Update,
                ),
            };
            let _ = tx.send(res);
            let _ = self.tx_bump.send(updates).await;
        }
        Ok(())
    }
}

struct YcsbFront {
    rx_op: Receiver<(YcsbOp, oneshot::Sender<YcsbRes>)>,
    tx_storage: Sender<StorageOp>,
    tx_inflight: Sender<(oneshot::Sender<YcsbRes>, InflightData)>,
    rx_bump: Receiver<BumpUpdates>,

    num_inflight: u64,
}

impl YcsbFront {
    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op((YcsbOp, oneshot::Sender<YcsbRes>)),
                Bump(BumpUpdates),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(bump) = self.rx_bump.recv() => Event::Bump(bump),
                else => break,
            } {
                Event::Op((op, tx)) => {
                    let data = match op {
                        YcsbOp::Read(key) => {
                            let (tx_fetch, rx_fetch) = oneshot::channel();
                            let _ = self
                                .tx_storage
                                .send(StorageOp::FetchAhead(
                                    StorageKey::digest(key),
                                    self.num_inflight,
                                    tx_fetch,
                                ))
                                .await;
                            InflightData::Read(rx_fetch)
                        }
                        YcsbOp::Update(key, value) => InflightData::Update(key, value),
                    };
                    let _ = self.tx_inflight.send((tx, data)).await;
                    self.num_inflight += 1
                }
                Event::Bump(updates) => {
                    let _ = self.tx_storage.send(StorageOp::Bump(updates)).await;
                    self.num_inflight -= 1
                }
            }
        }
        Ok(())
    }
}

impl Ycsb {
    pub fn new(
        rx_op: Receiver<(YcsbOp, oneshot::Sender<YcsbRes>)>,
        tx_storage_sched: Sender<StorageOp>,
    ) -> Self {
        let (tx_inflight, rx_inflight) = channel(100);
        let (tx_bump, rx_bump) = channel(100);
        Self {
            front: YcsbFront {
                rx_op,
                tx_storage: tx_storage_sched,
                tx_inflight,
                rx_bump,
                num_inflight: 0,
            },
            execute: YcsbExecute {
                rx_inflight,
                tx_bump,
            },
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let front = async {
            cancel
                .run_until_cancelled(self.front.run())
                .await
                .unwrap_or(Ok(()))
        };
        let execute = async {
            cancel
                .run_until_cancelled(self.execute.run())
                .await
                .unwrap_or(Ok(()))
        };
        try_join!(front, execute)?;
        Ok(())
    }
}
