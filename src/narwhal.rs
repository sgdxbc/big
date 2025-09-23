use std::{collections::HashMap, fmt::Debug};

use bincode::{Decode, Encode};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::message::Message;

pub type Round = u64;
pub type NodeIndex = u16;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Debug for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockHash(0x{}...)", &self.to_hex()[..8])
    }
}

#[derive(Clone)]
pub struct NarwhalConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,
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
    propose: Propose,
    certify: Certify,
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
        for BlockHash(link_hash) in &self.links {
            hasher.update(link_hash)
        }
        for txn in &self.txns {
            hasher.update(txn)
        }
        BlockHash(hasher.finalize().into())
    }

    fn from_network(block: &message::Block) -> Self {
        Self {
            round: block.round,
            node_index: block.creator_index,
            links: block.certs.iter().map(|cert| cert.block_hash).collect(),
            txns: block
                .txns
                .iter()
                .map(|t| Bytes::copy_from_slice(t))
                .collect(),
        }
    }
}

impl Narwhal {
    pub fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        bias_bullshark_anchor: bool,
        rx_txn: Receiver<Bytes>,
        tx_block: Sender<Block>,
        rx_message: Receiver<Message>,
        tx_message: Sender<Message>,
    ) -> Self {
        let (tx_cert, rx_cert) = channel(100);
        let (tx_proposed_block, rx_proposed_block) = channel(100);
        let propose = Propose::new(
            config.clone(),
            node_index,
            bias_bullshark_anchor,
            rx_txn,
            rx_cert,
            tx_message.clone(),
            tx_proposed_block,
        );
        let (tx_block_ok, rx_block_ok) = channel(100);
        let (tx_certified, rx_certified) = channel(100);
        let certify = Certify::new(
            config.clone(),
            node_index,
            rx_proposed_block,
            rx_block_ok,
            tx_certified,
        );
        Self {
            config,
            node_index,
            tx_block,
            rx_message,
            tx_message,
            workers: Some(Workers { propose, certify }),
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
        try_join!(task, workers.propose.run(cancel.clone()))?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

struct Propose {
    config: NarwhalConfig,
    node_index: NodeIndex,
    bias_bullshark_anchor: bool,

    rx_txn: Receiver<Bytes>,
    rx_cert: Receiver<message::Cert>,
    tx_message: Sender<Message>,
    tx_proposed_block: Sender<(Round, BlockHash)>,

    round: Round,
    txn_pool: Vec<Bytes>,
    certs: HashMap<Round, HashMap<NodeIndex, message::Cert>>,
}

impl Propose {
    fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        bias_bullshark_anchor: bool,
        rx_txn: Receiver<Bytes>,
        rx_cert: Receiver<message::Cert>,
        tx_message: Sender<Message>,
        tx_proposed_block: Sender<(Round, BlockHash)>,
    ) -> Self {
        Self {
            config,
            node_index,
            bias_bullshark_anchor,
            rx_txn,
            rx_cert,
            tx_message,
            tx_proposed_block,
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
                creator_index: self.node_index,
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
            let block_hash = Block::from_network(&block).hash();
            let _ = self.tx_proposed_block.send((self.round, block_hash)).await;
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
                        round_certs.insert(cert.creator_index, cert);
                        if round_certs.len()
                            >= (self.config.num_node - self.config.num_faulty_node) as usize
                            && (!self.bias_bullshark_anchor
                                || round_certs.contains_key(
                                    &((cert_round % self.config.num_node as Round) as NodeIndex),
                                ))
                        {
                            if self.round != cert_round {
                                warn!(
                                    "[{}] skip round {} -> {}",
                                    self.node_index,
                                    self.round,
                                    cert_round + 1
                                )
                            }
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

struct Certify {
    config: NarwhalConfig,
    node_index: NodeIndex,

    rx_block: Receiver<(Round, BlockHash)>,
    rx_block_ok: Receiver<message::BlockOk>,
    tx_certified: Sender<(BlockHash, message::Cert)>,

    certifying: Option<CertifyingState>,
}

struct CertifyingState {
    round: Round,
    block_hash: BlockHash,
    sigs: HashMap<NodeIndex, Vec<u8>>,
}

impl Certify {
    fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        rx_block: Receiver<(Round, BlockHash)>,
        rx_block_ok: Receiver<message::BlockOk>,
        tx_certified: Sender<(BlockHash, message::Cert)>,
    ) -> Self {
        Self {
            config,
            node_index,
            rx_block,
            rx_block_ok,
            tx_certified,
            certifying: None,
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Block((Round, BlockHash)),
                BlockOk(message::BlockOk),
            }
            match select! {
                Some(block) = self.rx_block.recv() => Event::Block(block),
                Some(block_ok) = self.rx_block_ok.recv() => Event::BlockOk(block_ok),
                else => break,
            } {
                Event::Block((round, block_hash)) => {
                    let replaced = self.certifying.replace(CertifyingState {
                        round,
                        block_hash,
                        sigs: Default::default(),
                    });
                    if let Some(replaced) = replaced {
                        warn!(
                            "block {} {:?} replaced before certified",
                            replaced.round, replaced.block_hash
                        )
                    }
                }
                Event::BlockOk(block_ok) => {
                    let Some(state) = &mut self.certifying else {
                        continue;
                    };
                    assert!(block_ok.round <= state.round);
                    if block_ok.round != state.round {
                        continue;
                    }
                    if block_ok.hash != state.block_hash
                        || block_ok.creator_index != self.node_index
                    {
                        warn!("invalid BlockOk for round {}", block_ok.round);
                        continue;
                    }

                    state.sigs.insert(block_ok.validator_index, block_ok.sig);
                    if state.sigs.len()
                        == (self.config.num_node - self.config.num_faulty_node) as usize
                    {
                        let certifying = self.certifying.take().unwrap();
                        let cert = message::Cert {
                            round: certifying.round,
                            creator_index: self.node_index,
                            block_hash: certifying.block_hash,
                            sigs: certifying.sigs.into_iter().collect(),
                        };
                        let _ = self.tx_certified.send((certifying.block_hash, cert)).await;
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
        pub creator_index: NodeIndex,
        pub certs: Vec<Cert>,
        pub txns: Vec<Vec<u8>>, // mean to be Vec<Bytes>
    }

    #[derive(Debug, Encode, Decode)]
    pub struct BlockOk {
        pub hash: BlockHash,
        pub round: Round,
        pub creator_index: NodeIndex,
        pub validator_index: NodeIndex,
        pub sig: Vec<u8>, // TODO
    }

    #[derive(Debug, Encode, Decode)]
    pub struct Cert {
        pub block_hash: BlockHash,
        pub round: Round,
        pub creator_index: NodeIndex,
        pub sigs: Vec<(NodeIndex, Vec<u8>)>,
    }
}
