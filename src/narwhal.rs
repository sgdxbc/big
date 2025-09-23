use std::{collections::HashMap, fmt::Debug};

use bincode::{Decode, Encode};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        watch,
    },
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
    tx_message: Sender<(SendTo, Message)>,

    workers: Option<Workers>,
}

pub enum SendTo {
    All,
    Node(NodeIndex),
}

struct Workers {
    propose: Propose,
    certify: Certify,
    validate: Validate,
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
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        let (tx_cert, rx_cert) = channel(100);
        // Certify use this channel to keep track of the (most recent) proposed block so it only
        // needs to work on single block
        // Validate use this channel to advance its round (in order to ignore old blocks) and to
        // issue loopback BlockOk to Certify
        let (tx_proposed, rx_proposed) = watch::channel((0, BlockHash([0; 32])));
        let propose = Propose::new(
            config.clone(),
            node_index,
            bias_bullshark_anchor,
            rx_txn,
            rx_cert,
            tx_message.clone(),
            tx_proposed,
        );
        let (tx_block_ok, rx_block_ok) = channel(100);
        let (tx_cert, rx_cert) = channel(100);
        let certify = Certify::new(
            config.clone(),
            node_index,
            rx_proposed.clone(),
            rx_block_ok,
            tx_cert.clone(),
        );
        let (tx_received_block, rx_received_block) = channel(100);
        let validate = Validate::new(
            node_index,
            rx_received_block,
            rx_proposed,
            tx_message.clone(),
            tx_block_ok.clone(),
        );
        Self {
            config,
            node_index,
            tx_block,
            rx_message,
            tx_message,
            workers: Some(Workers {
                propose,
                certify,
                validate,
            }),
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
        try_join!(
            task,
            workers.propose.run(cancel.clone()),
            workers.certify.run(cancel.clone()),
            workers.validate.run(cancel.clone()),
        )?;
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
    tx_message: Sender<(SendTo, Message)>,
    tx_proposed: watch::Sender<(Round, BlockHash)>,

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
        tx_message: Sender<(SendTo, Message)>,
        tx_proposed: watch::Sender<(Round, BlockHash)>,
    ) -> Self {
        Self {
            config,
            node_index,
            bias_bullshark_anchor,
            rx_txn,
            rx_cert,
            tx_message,
            tx_proposed,
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
            let _ = self.tx_proposed.send((self.round, block_hash));
            let _ = self
                .tx_message
                .send((SendTo::All, Message::Block(block)))
                .await;
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

    rx_proposed: watch::Receiver<(Round, BlockHash)>,
    rx_block_ok: Receiver<message::BlockOk>,
    tx_cert: Sender<message::Cert>,

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
        rx_proposed: watch::Receiver<(Round, BlockHash)>,
        rx_block_ok: Receiver<message::BlockOk>,
        tx_cert: Sender<message::Cert>,
    ) -> Self {
        Self {
            config,
            node_index,
            rx_proposed,
            rx_block_ok,
            tx_cert,
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
                Proposed((Round, BlockHash)),
                BlockOk(message::BlockOk),
            }
            match select! {
                Ok(()) = self.rx_proposed.changed() => Event::Proposed(*self.rx_proposed.borrow_and_update()),
                Some(block_ok) = self.rx_block_ok.recv() => Event::BlockOk(block_ok),
                else => break,
            } {
                Event::Proposed((round, block_hash)) => {
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
                        let _ = self.tx_cert.send(cert).await;
                    }
                }
            }
        }
        Ok(())
    }
}

struct Validate {
    node_index: NodeIndex,

    rx_block: Receiver<message::Block>,
    rx_proposed: watch::Receiver<(Round, BlockHash)>,
    tx_message: Sender<(SendTo, Message)>,
    tx_loopback_block_ok: Sender<message::BlockOk>,

    round: Round,
    proposed_blocks: HashMap<Round, HashMap<NodeIndex, BlockHash>>,
}

impl Validate {
    fn new(
        node_index: NodeIndex,
        rx_block: Receiver<message::Block>,
        rx_proposed: watch::Receiver<(Round, BlockHash)>,
        tx_message: Sender<(SendTo, Message)>,
        tx_loopback_block_ok: Sender<message::BlockOk>,
    ) -> Self {
        Self {
            node_index,
            rx_block,
            rx_proposed,
            tx_message,
            tx_loopback_block_ok,
            round: 0,
            proposed_blocks: Default::default(),
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
                Proposed((Round, BlockHash)),
                Block(message::Block),
            }
            match select! {
                Ok(())= self.rx_proposed.changed() => Event::Proposed(*self.rx_proposed.borrow_and_update()),
                Some(block) = self.rx_block.recv() => Event::Block(block),
                else => break,
            } {
                Event::Proposed((round, block_hash)) => {
                    assert!(round > self.round);
                    self.round = round;
                    self.proposed_blocks.retain(|&r, _| r >= round);
                    let block_ok = message::BlockOk {
                        hash: block_hash,
                        round,
                        creator_index: self.node_index,
                        validator_index: self.node_index,
                        sig: vec![], // TODO
                    };
                    let _ = self.tx_loopback_block_ok.send(block_ok).await;
                }
                Event::Block(block) => {
                    if block.round < self.round {
                        continue;
                    }
                    // TODO verify block
                    let round_blocks = self.proposed_blocks.entry(block.round).or_default();
                    if round_blocks.contains_key(&block.creator_index) {
                        warn!(
                            "[{}] duplicate block for round {} from {}",
                            self.node_index, block.round, block.creator_index
                        );
                        continue;
                    }
                    let block_hash = Block::from_network(&block).hash();
                    round_blocks.insert(block.creator_index, block_hash);
                    let block_ok = message::BlockOk {
                        hash: block_hash,
                        round: block.round,
                        creator_index: block.creator_index,
                        validator_index: self.node_index,
                        sig: vec![], // TODO
                    };
                    let _ = self
                        .tx_message
                        .send((
                            SendTo::Node(block.creator_index),
                            Message::BlockOk(block_ok),
                        ))
                        .await;
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

    #[derive(Debug, Clone, Encode, Decode)]
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

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Cert {
        pub block_hash: BlockHash,
        pub round: Round,
        pub creator_index: NodeIndex,
        pub sigs: Vec<(NodeIndex, Vec<u8>)>,
    }
}
