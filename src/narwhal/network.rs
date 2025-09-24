use std::net::SocketAddr;

use bincode::config::standard;
use bytes::Bytes;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

use crate::network::{Mesh, Network, NetworkId};

use super::{Block, NarwhalConfig, NodeIndex, SendTo, message::Message};

struct Incoming {
    rx_bytes: Receiver<(NetworkId, Vec<u8>)>,
    tx_message: Sender<Message>,
}

impl Incoming {
    fn new(rx_bytes: Receiver<(NetworkId, Vec<u8>)>, tx_message: Sender<Message>) -> Self {
        Self {
            rx_bytes,
            tx_message,
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        while let Some(Some((_, bytes))) = cancel.run_until_cancelled(self.rx_bytes.recv()).await {
            let (message, len) = bincode::decode_from_slice(&bytes, standard())?;
            anyhow::ensure!(len == bytes.len());
            let _ = self.tx_message.send(message).await;
        }
        Ok(())
    }
}

struct Outgoing {
    node_table: Vec<NetworkId>, // node index -> network id
    node_index: NodeIndex,

    rx_message: Receiver<(SendTo, Message)>,
    tx_bytes: Sender<(NetworkId, Bytes)>,
}

impl Outgoing {
    fn new(
        node_table: Vec<NetworkId>,
        node_index: NodeIndex,
        rx_message: Receiver<(SendTo, Message)>,
        tx_bytes: Sender<(NetworkId, Bytes)>,
    ) -> Self {
        Self {
            node_table,
            node_index,
            rx_message,
            tx_bytes,
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        while let Some(Some((send_to, message))) =
            cancel.run_until_cancelled(self.rx_message.recv()).await
        {
            let bytes = Bytes::from(bincode::encode_to_vec(&message, standard())?);
            match send_to {
                SendTo::All => {
                    for (node_index, &network_id) in self.node_table.iter().enumerate() {
                        if node_index as NodeIndex == self.node_index {
                            continue;
                        }
                        let _ = self.tx_bytes.send((network_id, bytes.clone())).await;
                    }
                }
                SendTo::Node(node_index) => {
                    assert_ne!(node_index, self.node_index);
                    let _ = self
                        .tx_bytes
                        .send((self.node_table[node_index as usize], bytes))
                        .await;
                }
            }
        }
        Ok(())
    }
}

pub struct Narwhal {
    tx_mesh_established: oneshot::Sender<()>,

    core: super::Narwhal,
    incoming: Incoming,
    outgoing: Outgoing,
    network: Network,
    mesh: Mesh,
}

impl Narwhal {
    pub fn new(
        config: NarwhalConfig,
        node_index: NodeIndex,
        node_table: Vec<NetworkId>,
        addrs: Vec<SocketAddr>,
        network_id: NetworkId,
        rx_txn: Receiver<Bytes>,
        tx_block: Sender<Block>,
        tx_mesh_established: oneshot::Sender<()>,
    ) -> Self {
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = channel(100);
        let (tx_connection, rx_connection) = channel(100);
        let (tx_incoming_bytes, rx_incoming_bytes) = channel(100);
        let (tx_outgoing_bytes, rx_outgoing_bytes) = channel(100);

        let core = super::Narwhal::new(
            config,
            node_index,
            rx_txn,
            tx_block,
            rx_incoming_message,
            tx_outgoing_message,
        );
        let incoming = Incoming::new(rx_incoming_bytes, tx_incoming_message);
        let outgoing = Outgoing::new(
            node_table,
            node_index,
            rx_outgoing_message,
            tx_outgoing_bytes,
        );
        let network = Network::new(rx_connection, rx_outgoing_bytes, tx_incoming_bytes);
        let mesh = Mesh::new(addrs, network_id, tx_connection);
        Self {
            tx_mesh_established,
            core,
            incoming,
            outgoing,
            network,
            mesh,
        }
    }

    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let task = async {
            let Some(result) = cancel.run_until_cancelled(self.mesh.run()).await else {
                return Ok(());
            };
            result?;
            let _ = self.tx_mesh_established.send(());
            try_join!(
                self.core.run(cancel.clone()),
                self.incoming.run(cancel.clone()),
                self.outgoing.run(cancel.clone())
            )?;
            Ok(())
        };
        try_join!(task, self.network.run(cancel.clone()))?;
        Ok(())
    }
}
