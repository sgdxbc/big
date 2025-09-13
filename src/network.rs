use std::{collections::HashMap, net::SocketAddr};

use bytes::Bytes;
use quinn::{Connection, ConnectionError, Endpoint};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::cert::{client_config, server_config};

pub type NetworkId = u32;

pub struct Network {
    id: NetworkId,

    cancel: CancellationToken,
    rx_incoming_connection: Receiver<Connection>,
    rx_outgoing_connection: Receiver<(NetworkId, Connection)>,
    rx_message: Receiver<(NetworkId, Bytes)>,
    tx_message: Sender<(NetworkId, Vec<u8>)>,

    connections: HashMap<NetworkId, Connection>,
    tasks: JoinSet<anyhow::Result<()>>,
    tx_close: Sender<NetworkId>,
    rx_close: Receiver<NetworkId>,
}

impl Network {
    pub fn new(
        id: NetworkId,
        cancel: CancellationToken,
        rx_incoming_connection: Receiver<Connection>,
        rx_outgoing_connection: Receiver<(NetworkId, Connection)>,
        rx_message: Receiver<(NetworkId, Bytes)>,
        tx_message: Sender<(NetworkId, Vec<u8>)>,
    ) -> Self {
        let (tx_close, rx_close) = channel(1);
        Self {
            id,
            cancel,
            rx_incoming_connection,
            rx_outgoing_connection,
            rx_message,
            tx_message,
            connections: Default::default(),
            tasks: JoinSet::new(),
            tx_close,
            rx_close,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.cancel
            .clone()
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<R> {
                IncomingConnection(Connection),
                OutgoingConnection((NetworkId, Connection)),
                Close(NetworkId),
                Message((NetworkId, Bytes)),
                TaskResult(R),
            }
            match select! {
                Some(connection) = self.rx_incoming_connection.recv() => Event::IncomingConnection(connection),
                Some((id, connection)) = self.rx_outgoing_connection.recv() => Event::OutgoingConnection((id, connection)),
                Some(id) = self.rx_close.recv() => Event::Close(id),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                Some(result) = self.tasks.join_next() => Event::TaskResult(result),
                else => break,
            } {
                Event::IncomingConnection(connection) => {
                    let mut id = [0; size_of::<NetworkId>()];
                    connection.accept_uni().await?.read_exact(&mut id).await?;
                    self.handle_connection(NetworkId::from_le_bytes(id), connection)
                }
                Event::OutgoingConnection((remote_id, connection)) => {
                    connection
                        .open_uni()
                        .await?
                        .write_all(&self.id.to_le_bytes())
                        .await?;
                    self.handle_connection(remote_id, connection)
                }
                Event::Close(id) => {
                    self.connections.remove(&id);
                }
                Event::Message((id, message)) => {
                    let Some(connection) = self.connections.get(&id) else {
                        warn!("send message to unknown network id {id:08x}");
                        continue;
                    };
                    let connection = connection.clone();
                    self.tasks.spawn(async move {
                        let mut send_stream = connection.open_uni().await?;
                        send_stream.write_all(&message).await?;
                        Ok(())
                    });
                }
                Event::TaskResult(result) => result??,
            }
        }
        Ok(())
    }

    fn handle_connection(&mut self, id: u32, connection: Connection) {
        self.connections.insert(id, connection.clone());
        let tx_message = self.tx_message.clone();
        let tx_close = self.tx_close.clone();
        self.tasks
            .spawn(Self::read_connection(id, connection, tx_message, tx_close));
    }

    async fn read_connection(
        id: NetworkId,
        connection: Connection,
        tx_message: Sender<(NetworkId, Vec<u8>)>,
        tx_close: Sender<NetworkId>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv_stream = match connection.accept_uni().await {
                Ok(stream) => stream,
                Err(ConnectionError::ApplicationClosed(_)) => break,
                Err(err) => Err(err)?,
            };
            let message = recv_stream.read_to_end(64 << 20).await?;
            let _ = tx_message.send((id, message.to_vec())).await;
        }
        let _ = tx_close.send(id).await;
        Ok(())
    }
}

pub struct Mesh {
    addrs: Vec<SocketAddr>,
    id: NetworkId,

    tx_outgoing_connection: Sender<(NetworkId, Connection)>,
    tx_incoming_connection: Sender<Connection>,
}

impl Mesh {
    pub fn new(
        addrs: Vec<SocketAddr>,
        id: NetworkId,
        tx_outgoing_connection: Sender<(NetworkId, Connection)>,
        tx_incoming_connection: Sender<Connection>,
    ) -> Self {
        Self {
            addrs,
            id,
            tx_outgoing_connection,
            tx_incoming_connection,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut endpoint = Endpoint::server(server_config(), self.addrs[self.id as usize])?;
        endpoint.set_default_client_config(client_config());
        let mut tasks = JoinSet::new();
        let num_connection = self.addrs.len() - 1;
        for (remote_id, addr) in self.addrs.into_iter().take(self.id as _).enumerate() {
            let endpoint = endpoint.clone();
            let tx_outgoing_connection = self.tx_outgoing_connection.clone();
            tasks.spawn(async move {
                let connection = endpoint.connect(addr, "server.example")?.await?;
                let _ = tx_outgoing_connection
                    .send((remote_id as _, connection))
                    .await;
                anyhow::Ok(())
            });
        }
        tasks.spawn(async move {
            for _ in 0..num_connection - self.id as usize {
                let connection = endpoint
                    .accept()
                    .await
                    .ok_or(anyhow::format_err!("endpoint closed"))?
                    .await?;
                let _ = self.tx_incoming_connection.send(connection).await;
            }
            Ok(())
        });
        while let Some(res) = tasks.join_next().await {
            res??
        }
        Ok(())
    }
}
