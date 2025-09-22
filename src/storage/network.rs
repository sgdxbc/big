use std::{net::SocketAddr, sync::Arc};

use bincode::config::standard;
use bytes::Bytes;
use rocksdb::DB;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

use crate::network::{Mesh, Network, NetworkId};

use super::{NodeIndex, SendTo, StorageConfig, StorageOp, message::Message};

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
    node_indices: Vec<NodeIndex>,

    rx_message: Receiver<(SendTo, Message)>,
    tx_bytes: Sender<(NetworkId, Bytes)>,
}

impl Outgoing {
    fn new(
        node_table: Vec<NetworkId>,
        node_indices: Vec<NodeIndex>,
        rx_message: Receiver<(SendTo, Message)>,
        tx_bytes: Sender<(NetworkId, Bytes)>,
    ) -> Self {
        Self {
            node_table,
            node_indices,
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
                    for (node_id, &network_id) in self.node_table.iter().enumerate() {
                        if self.node_indices.contains(&(node_id as _)) {
                            continue;
                        }
                        let _ = self.tx_bytes.send((network_id, bytes.clone())).await;
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct Storage {
    tx_mesh_established: oneshot::Sender<()>,

    core: super::Storage,
    incoming: Incoming,
    outgoing: Outgoing,
    network: Network,
    mesh: Mesh,
}

impl Storage {
    pub fn new(
        config: StorageConfig,
        node_indices: Vec<NodeIndex>,
        db: Arc<DB>,
        node_table: Vec<NetworkId>,
        addrs: Vec<SocketAddr>,
        network_id: NetworkId,
        rx_op: Receiver<StorageOp>,
        tx_mesh_established: oneshot::Sender<()>,
    ) -> Self {
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = channel(100);
        let (tx_connection, rx_connection) = channel(100);
        let (tx_incoming_bytes, rx_incoming_bytes) = channel(100);
        let (tx_outgoing_bytes, rx_outgoing_bytes) = channel(100);

        let core = super::Storage::new(
            config,
            node_indices.clone(),
            db,
            rx_op,
            rx_incoming_message,
            tx_outgoing_message,
        );
        let incoming = Incoming::new(rx_incoming_bytes, tx_incoming_message);
        let outgoing = Outgoing::new(
            node_table,
            node_indices,
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
