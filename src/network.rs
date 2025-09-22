use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use quinn::{Connection, ConnectionError, Endpoint};
use tokio::{
    select,
    sync::{
        Barrier,
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::cert::{client_config, server_config};

pub type NetworkId = u32;

pub struct Network {
    rx_peer: Receiver<(NetworkId, Connection, oneshot::Sender<()>)>,
    rx_message: Receiver<(NetworkId, Bytes)>,
    tx_message: Sender<(NetworkId, Vec<u8>)>,

    connections: HashMap<NetworkId, Connection>,
    tasks: JoinSet<anyhow::Result<()>>,
    tx_close: Sender<NetworkId>,
    rx_close: Receiver<NetworkId>,
}

impl Network {
    pub fn new(
        rx_peer: Receiver<(NetworkId, Connection, oneshot::Sender<()>)>,
        rx_message: Receiver<(NetworkId, Bytes)>,
        tx_message: Sender<(NetworkId, Vec<u8>)>,
    ) -> Self {
        let (tx_close, rx_close) = channel(1);
        Self {
            rx_peer,
            rx_message,
            tx_message,
            connections: Default::default(),
            tasks: JoinSet::new(),
            tx_close,
            rx_close,
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
            enum Event<R> {
                Connection((NetworkId, Connection, oneshot::Sender<()>)),
                Close(NetworkId),
                Message((NetworkId, Bytes)),
                TaskResult(R),
            }
            match select! {
                Some(peer) = self.rx_peer.recv() => Event::Connection(peer),
                Some(id) = self.rx_close.recv() => Event::Close(id),
                Some(message) = self.rx_message.recv() => Event::Message(message),
                Some(result) = self.tasks.join_next() => Event::TaskResult(result),
                else => break,
            } {
                Event::Connection((id, connection, tx_ok)) => {
                    self.handle_connection(id, connection);
                    let _ = tx_ok.send(());
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
                        let mut send_stream = match connection.open_uni().await {
                            Ok(stream) => stream,
                            Err(ConnectionError::ApplicationClosed(_)) => {
                                warn!("connection to {id:08x} closed; message dropped");
                                return Ok(());
                            }
                            Err(err) => Err(err)?,
                        };
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
        let mut tasks = JoinSet::new();
        loop {
            enum Event<S, R> {
                Accept(S),
                TaskResult(R),
            }
            match select! {
                accept = connection.accept_uni() => Event::Accept(accept),
                Some(result) = tasks.join_next() => Event::TaskResult(result),
            } {
                Event::Accept(Ok(mut recv_stream)) => {
                    let tx_message = tx_message.clone();
                    tasks.spawn(async move {
                        let _ = tx_message
                            .send((id, recv_stream.read_to_end(64 << 20).await?))
                            .await;
                        anyhow::Ok(())
                    });
                }
                Event::Accept(Err(ConnectionError::ApplicationClosed(_))) => break,
                Event::Accept(Err(err)) => Err(err)?,
                Event::TaskResult(result) => result??,
            }
        }
        info!("connection to {id:08x} closed");
        let _ = tx_close.send(id).await;
        Ok(())
    }
}

pub struct Mesh {
    addrs: Vec<SocketAddr>,
    id: NetworkId,

    tx_connection: Sender<(NetworkId, Connection, oneshot::Sender<()>)>,
}

impl Mesh {
    pub fn new(
        addrs: Vec<SocketAddr>,
        id: NetworkId,
        tx_connection: Sender<(NetworkId, Connection, oneshot::Sender<()>)>,
    ) -> Self {
        Self {
            addrs,
            id,
            tx_connection,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let mut endpoint = Endpoint::server(server_config(), self.addrs[self.id as usize])?;
        endpoint.set_default_client_config(client_config());
        let mut tasks = JoinSet::new();
        let num_connection = self.addrs.len() - 1;
        let barrier = Arc::new(Barrier::new(self.id as usize + 1));
        for (remote_id, addr) in self.addrs.into_iter().take(self.id as _).enumerate() {
            let endpoint = endpoint.clone();
            let tx_connection = self.tx_connection.clone();
            let id = self.id;
            let barrier = barrier.clone();
            tasks.spawn(async move {
                let connection = endpoint.connect(addr, "server.example")?.await?;
                barrier.wait().await;
                connection
                    .open_uni()
                    .await?
                    .write_all(&id.to_le_bytes())
                    .await?;
                let (tx_ok, rx_ok) = oneshot::channel();
                let _ = tx_connection
                    .send((remote_id as _, connection, tx_ok))
                    .await;
                let _ = rx_ok.await;
                anyhow::Ok(())
            });
        }
        tasks.spawn(async move {
            let mut connections = Vec::new();
            for _ in 0..num_connection - self.id as usize {
                let connection = endpoint
                    .accept()
                    .await
                    .ok_or(anyhow::format_err!("endpoint closed"))?
                    .await?;
                connections.push(connection)
            }
            barrier.wait().await;
            for connection in connections {
                let mut id = [0; size_of::<NetworkId>()];
                connection.accept_uni().await?.read_exact(&mut id).await?;
                let (tx_ok, rx_ok) = oneshot::channel();
                let _ = self
                    .tx_connection
                    .send((NetworkId::from_le_bytes(id), connection, tx_ok))
                    .await;
                let _ = rx_ok.await;
            }
            Ok(())
        });
        while let Some(res) = tasks.join_next().await {
            res??
        }
        info!("mesh connection established");
        Ok(())
    }
}
