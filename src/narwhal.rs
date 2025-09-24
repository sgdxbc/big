use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem::take,
};

use bincode::{Decode, Encode};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use self::message::Message;

pub mod network;

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

    rx_txn: Receiver<Bytes>,
    rx_message: Receiver<Message>,
    tx_message: Sender<(SendTo, Message)>,

    round: Round,
    block_hash: Option<BlockHash>, // None if proposal for current round has certified
    txn_pool: Vec<Bytes>,
    block_oks: HashMap<NodeIndex, message::BlockOk>,
    certs: HashMap<Round, HashMap<NodeIndex, message::Cert>>,
    reorder_validate: HashMap<Round, Vec<(NodeIndex, BlockHash)>>,

    workers: Option<Workers>,
    tx_certifying_block: Sender<Block>,
    tx_certified: Sender<BlockHash>,
}

pub enum SendTo {
    All,
    Node(NodeIndex),
}

struct Workers {
    dag: Dag,
}

#[derive(Debug, Clone)]
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
        rx_txn: Receiver<Bytes>,
        tx_block: Sender<Block>,
        rx_message: Receiver<Message>,
        tx_message: Sender<(SendTo, Message)>,
    ) -> Self {
        let (tx_certifying_block, rx_certifying_block) = channel(100);
        let (tx_certified, rx_certified) = channel(100);
        let dag = Dag::new(config.num_node, rx_certifying_block, rx_certified, tx_block);
        Self {
            config,
            node_index,
            rx_txn,
            rx_message,
            tx_message,
            round: 0,
            block_hash: None,
            txn_pool: Default::default(),
            block_oks: Default::default(),
            certs: Default::default(),
            reorder_validate: Default::default(),
            workers: Some(Workers { dag }),
            tx_certifying_block,
            tx_certified,
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
        try_join!(task, workers.dag.run(cancel.clone()),)?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        self.propose().await;
        loop {
            enum Event {
                Txn(Bytes),
                Message(Message),
            }
            match select! {
                Some(txn) = self.rx_txn.recv() => Event::Txn(txn),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                else => break,
            } {
                Event::Txn(txn) => self.txn_pool.push(txn),
                Event::Message(message) => self.handle_message(message).await,
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) {
        trace!("[{}] {message:?}", self.node_index);
        match message {
            Message::Block(network_block) => {
                let block = Block::from_network(&network_block);
                let _ = self.tx_certifying_block.send(block.clone()).await;
                self.validate(&block).await
                // let _ = self.tx_received_block.send(block).await;
            }
            Message::BlockOk(block_ok) => {
                assert!(block_ok.round <= self.round);
                if block_ok.round == self.round {
                    self.insert_block_ok(block_ok).await
                }
            }
            Message::Cert(cert) => self.handle_cert(cert).await,
        }
    }

    async fn handle_cert(&mut self, cert: message::Cert) {
        let _ = self.tx_certified.send(cert.block_hash).await;
        let cert_round = cert.round;
        if cert_round < self.round {
            return;
        }
        let round_certs = self.certs.entry(cert_round).or_default();
        round_certs.insert(cert.creator_index, cert);
        if round_certs.len() >= (self.config.num_node - self.config.num_faulty_node) as usize
        // TODO may need to restrict DAG shape
        {
            if cert_round > self.round {
                warn!(
                    "fast-forwarding from round {} to {}",
                    self.round,
                    cert_round + 1
                );
            }
            self.round = cert_round + 1;
            trace!("[{}] advanced to round {}", self.node_index, self.round);
            self.certs.retain(|&r, _| r >= cert_round);
            self.propose().await;
            self.reorder_validate.retain(|&r, _| r >= self.round);
            if let Some(pending) = self.reorder_validate.remove(&self.round) {
                for (node_index, block_hash) in pending {
                    self.validate2(node_index, block_hash).await
                }
            }
        }
    }

    async fn propose(&mut self) {
        if let Some(block_hash) = self.block_hash {
            debug!("[{}] interrupted proposal {block_hash:?}", self.node_index);
            self.block_oks.clear()
        }
        let certs = if self.round == 0 {
            Default::default()
        } else {
            self.certs.remove(&(self.round - 1)).unwrap()
        };
        assert!(certs.iter().all(|(_, cert)| cert.round == self.round - 1));
        let network_block = message::Block {
            round: self.round,
            creator_index: self.node_index,
            certs: certs.into_values().collect(),
            // TODO limit number of txns
            txns: self.txn_pool.drain(..).map(Into::into).collect(),
        };
        let block = Block::from_network(&network_block);
        let _ = self
            .tx_message
            .send((SendTo::All, Message::Block(network_block)))
            .await;
        self.block_hash = Some(block.hash());
        let _ = self.tx_certifying_block.send(block.clone()).await;
        self.validate(&block).await
    }

    async fn validate(&mut self, block: &Block) {
        if block.round < self.round {
            trace!(
                "[{}] ignoring old block for round {} < {}",
                self.node_index, block.round, self.round
            );
            return;
        }
        // TODO verify integrity
        let block_hash = block.hash();
        if block.round == self.round {
            self.validate2(block.node_index, block_hash).await
        } else {
            self.reorder_validate
                .entry(block.round)
                .or_default()
                .push((block.node_index, block_hash))
        }
    }

    async fn validate2(&mut self, node_index: NodeIndex, block_hash: BlockHash) {
        // TODO verify non-equivocation
        let block_ok = message::BlockOk {
            hash: block_hash,
            round: self.round,
            creator_index: node_index,
            validator_index: self.node_index,
            sig: vec![], // TODO
        };
        if node_index == self.node_index {
            self.insert_block_ok(block_ok).await
        } else {
            let _ = self
                .tx_message
                .send((SendTo::Node(node_index), Message::BlockOk(block_ok)))
                .await;
        }
    }

    async fn insert_block_ok(&mut self, block_ok: message::BlockOk) {
        assert!(block_ok.round == self.round);
        let Some(block_hash) = self.block_hash else {
            return;
        };
        if block_ok.hash != block_hash || block_ok.creator_index != self.node_index {
            warn!("invalid BlockOk for round {}", block_ok.round);
            return;
        }
        // TODO verify signature
        self.block_oks.insert(block_ok.validator_index, block_ok);
        if self.block_oks.len() == (self.config.num_node - self.config.num_faulty_node) as usize {
            trace!(
                "[{}] block {:?} certified for round {}",
                self.node_index, block_hash, self.round
            );
            let cert = message::Cert {
                round: self.round,
                creator_index: self.node_index,
                block_hash: self.block_hash.take().unwrap(),
                sigs: take(&mut self.block_oks)
                    .into_iter()
                    .map(|(node_index, block_ok)| (node_index, block_ok.sig))
                    .collect(),
            };
            let _ = self
                .tx_message
                .send((SendTo::All, Message::Cert(cert.clone())))
                .await;
            Box::pin(self.handle_cert(cert)).await
        }
    }
}

// better name?
struct Dag {
    num_node: NodeIndex,

    rx_block: Receiver<Block>,
    rx_certified: Receiver<BlockHash>,
    tx_block: Sender<Block>,

    certifying_blocks: HashMap<BlockHash, Block>,
    delivered: HashMap<Round, HashSet<BlockHash>>,
    reordering_blocks: HashMap<BlockHash, Vec<Block>>, // missing parent -> children
    reordering_certified: HashSet<BlockHash>,
}

impl Dag {
    fn new(
        num_node: NodeIndex,
        rx_block: Receiver<Block>,
        rx_certified: Receiver<BlockHash>,
        tx_block: Sender<Block>,
    ) -> Self {
        Self {
            num_node,
            rx_block,
            rx_certified,
            tx_block,
            certifying_blocks: Default::default(),
            delivered: Default::default(),
            reordering_blocks: Default::default(),
            reordering_certified: Default::default(),
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
                Block(Block),
                Certified(BlockHash),
            }
            match select! {
                Some(block) = self.rx_block.recv() => Event::Block(block),
                Some(block_hash) = self.rx_certified.recv() => Event::Certified(block_hash),
                else => break,
            } {
                Event::Block(block) => {
                    if self.reordering_certified.remove(&block.hash()) {
                        self.may_deliver(block).await
                    } else {
                        self.certifying_blocks.insert(block.hash(), block);
                    }
                }
                Event::Certified(block_hash) => {
                    let Some(block) = self.certifying_blocks.remove(&block_hash) else {
                        self.reordering_certified.insert(block_hash);
                        continue;
                    };
                    self.may_deliver(block).await;
                    // TODO garbage collect
                }
            }
        }
        Ok(())
    }

    async fn may_deliver(&mut self, block: Block) {
        for &link in &block.links {
            if !self
                .delivered
                .get(&(block.round - 1))
                .is_some_and(|delivered| delivered.contains(&link))
            {
                self.reordering_blocks.entry(link).or_default().push(block);
                return;
            }
        }
        let block_hash = block.hash();
        let round_delivered = self.delivered.entry(block.round).or_default();
        round_delivered.insert(block_hash);
        // a weak garbage collection rule that only effective without faulty nodes
        // just for evaluation purpose
        if round_delivered.len() == self.num_node as usize && block.round > 0 {
            self.delivered.remove(&(block.round - 1));
        }
        let _ = self.tx_block.send(block).await;
        if let Some(blocks) = self.reordering_blocks.remove(&block_hash) {
            for block in blocks {
                Box::pin(self.may_deliver(block)).await;
            }
        }
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

mod parse {
    use crate::parse::Extract;

    use super::NarwhalConfig;

    impl Extract for NarwhalConfig {
        fn extract(configs: &crate::parse::Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_node: configs.get("narwhal.num-node")?,
                num_faulty_node: configs.get("narwhal.num-faulty-node")?,
            })
        }
    }
}
