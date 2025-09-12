use std::collections::HashMap;

use bytes::Bytes;
use quinn::{Connection, ConnectionError};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
    task::{JoinError, JoinSet},
};
use tracing::warn;

pub type NetworkId = u32;

pub struct Network {
    rx_incoming_connection: Receiver<Connection>,
    rx_outgoing_connection: Receiver<(NetworkId, Connection)>,
    rx_message: Receiver<(NetworkId, Bytes)>,
    tx_message: Sender<(NetworkId, Vec<u8>)>,

    id: NetworkId,
    connections: HashMap<NetworkId, Connection>,
    tasks: JoinSet<anyhow::Result<()>>,
    tx_close: Sender<NetworkId>,
    rx_close: Receiver<NetworkId>,
}

impl Network {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                IncomingConnection(Connection),
                OutgoingConnection((NetworkId, Connection)),
                Close(NetworkId),
                Message((NetworkId, Bytes)),
                TaskResult(Result<anyhow::Result<()>, JoinError>),
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
