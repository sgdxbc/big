use std::collections::HashMap;

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    try_join,
};
use tokio_util::sync::CancellationToken;

use self::message::Message;

pub type Round = u64;
pub type NodeIndex = u16;
pub type BlockHash = [u8; 32];

#[derive(Clone)]
pub struct NarwhalConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,

    bias_bullshark_anchor: bool,
}

pub struct Narwhal {
    config: NarwhalConfig,
    node_index: NodeIndex,

    tx_block: Sender<Block>,
    rx_message: Receiver<Message>,
    tx_message: Sender<Message>, // always broadcast

    workers: Option<Workers>,
}

struct Workers {
    proposer: Proposer,
}

pub struct Block {
    pub round: Round,
    pub node_index: NodeIndex,
    pub links: Vec<BlockHash>,
    pub txns: Vec<Bytes>,
}

impl Block {
    pub fn hash(&self) -> BlockHash {
        let mut hasher = Sha256::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.node_index.to_le_bytes());
        for link in &self.links {
            hasher.update(link)
        }
        for txn in &self.txns {
            hasher.update(txn)
        }
        hasher.finalize().into()
    }
}

impl Narwhal {
    pub fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        rx_txn: Receiver<Bytes>,
        tx_block: Sender<Block>,
        rx_message: Receiver<Message>,
        tx_message: Sender<Message>,
    ) -> Self {
        let (tx_cert, rx_cert) = channel(100);
        let proposer = Proposer::new(
            config.clone(),
            node_index,
            rx_txn,
            rx_cert,
            tx_message.clone(),
        );
        Self {
            config,
            node_index,
            tx_block,
            rx_message,
            tx_message,
            workers: Some(Workers { proposer }),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let workers = self.workers.take().unwrap();
        let task = async {
            cancel
                .run_until_cancelled(self.run_inner())
                .await
                .unwrap_or(Ok(()))
        };
        try_join!(task, workers.proposer.run(cancel.clone()))?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct Proposer {
    config: NarwhalConfig,
    node_index: NodeIndex,

    rx_txn: Receiver<Bytes>,
    rx_cert: Receiver<message::Cert>,
    tx_message: Sender<Message>,

    round: Round,
    txn_pool: Vec<Bytes>,
    certs: HashMap<Round, HashMap<NodeIndex, message::Cert>>,
}

impl Proposer {
    fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        rx_txn: Receiver<Bytes>,
        rx_cert: Receiver<message::Cert>,
        tx_message: Sender<Message>,
    ) -> Self {
        Self {
            config,
            node_index,
            rx_txn,
            rx_cert,
            tx_message,
            round: 0,
            txn_pool: Default::default(),
            certs: Default::default(),
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        'outer: loop {
            let block = message::Block {
                round: self.round,
                node_index: self.node_index,
                certs: if self.round == 0 {
                    Default::default()
                } else {
                    self.certs
                        .remove(&(self.round - 1))
                        .unwrap()
                        .into_values()
                        .collect()
                },
                txns: self.txn_pool.drain(..).map(Into::into).collect(),
            };
            let _ = self.tx_message.send(Message::Block(block)).await;
            loop {
                enum Event {
                    Txn(Bytes),
                    Cert(message::Cert),
                }
                match select! {
                    Some(txn) = self.rx_txn.recv() => Event::Txn(txn),
                    Some(cert) = self.rx_cert.recv() => Event::Cert(cert),
                    else => break 'outer,
                } {
                    Event::Txn(txn) => self.txn_pool.push(txn),
                    Event::Cert(cert) => {
                        if cert.round < self.round {
                            continue;
                        }
                        let cert_round = cert.round;
                        let round_certs = self.certs.entry(cert_round).or_default();
                        round_certs.insert(cert.node_index, cert);
                        if round_certs.len()
                            == (self.config.num_node - self.config.num_faulty_node) as usize
                        {
                            self.round = cert_round + 1;
                            self.certs.retain(|&r, _| r >= cert_round);
                            continue 'outer;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub mod message {
    use bincode::{Decode, Encode};

    use super::{BlockHash, NodeIndex, Round};

    #[derive(Debug, Encode, Decode)]
    pub enum Message {
        Block(Block),
        BlockOk(BlockOk),
        Cert(Cert),
    }

    #[derive(Debug, Encode, Decode)]
    pub struct Block {
        pub round: Round,
        pub node_index: NodeIndex,
        pub certs: Vec<Cert>,
        pub txns: Vec<Vec<u8>>, // mean to be Vec<Bytes>
    }

    #[derive(Debug, Encode, Decode)]
    pub struct BlockOk {
        pub hash: BlockHash,
        pub node_index: NodeIndex,
        pub sig: Vec<u8>, // TODO
    }

    #[derive(Debug, Encode, Decode)]
    pub struct Cert {
        pub round: Round,
        pub node_index: NodeIndex,
        pub hash: BlockHash,
        pub sigs: Vec<(NodeIndex, Vec<u8>)>,
    }
}
