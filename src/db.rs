use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};

use bytes::Bytes;
use rocksdb::{DB, WriteBatch};
use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    oneshot,
};
use tokio_util::sync::CancellationToken;

use crate::storage2::StorageKey;

struct StopGuard(Sender<()>);

impl Drop for StopGuard {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

struct GetWorker {
    db: Arc<DB>,

    // we need mpmc here
    rx: flume::Receiver<(StorageKey, oneshot::Sender<Option<Bytes>>)>,
    running: Arc<AtomicBool>,
    _stop_guard: StopGuard,
}

impl GetWorker {
    fn run(self) -> anyhow::Result<()> {
        while self.running.load(Relaxed) {
            let (key, tx) = match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(get) => get,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            };
            let value = self.db.get(key)?;
            let _ = tx.send(value.map(Bytes::from));
        }
        Ok(())
    }
}

struct WriteWorker {
    db: Arc<DB>,

    // cannot use tokio channel because it does not support timeout on blocking interfaces
    // prefer flume over std just because we are already using flume channels. three different
    // kinds of channels in a module is too much
    rx: flume::Receiver<Vec<(StorageKey, Option<Bytes>)>>,
    tx: Sender<u64>,
    running: Arc<AtomicBool>,
    _stop_guard: StopGuard,
}

impl WriteWorker {
    fn run(self) -> anyhow::Result<()> {
        while self.running.load(Relaxed) {
            let mut updates = match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(get) => get,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            };
            let mut count = 1;
            for more_updates in self.rx.drain() {
                updates.extend(more_updates);
                count += 1
            }
            let mut batch = WriteBatch::new();
            for (key, value) in updates {
                match value {
                    Some(value) => batch.put(key, value),
                    None => batch.delete(key),
                }
            }
            if !batch.is_empty() {
                self.db.write(batch)?
            }
            let _ = self.tx.blocking_send(count);
        }
        Ok(())
    }
}

pub struct DbWorker {
    get_workers: Vec<GetWorker>,
    write_worker: WriteWorker,
    rx_stopped: Receiver<()>,
    running: Arc<AtomicBool>,
}

impl DbWorker {
    pub fn new(
        db: Arc<DB>,
        num_get_worker: u32,
        rx_get: flume::Receiver<(StorageKey, oneshot::Sender<Option<Bytes>>)>,
        rx_write: flume::Receiver<Vec<(StorageKey, Option<Bytes>)>>,
        tx_write_done: Sender<u64>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let (tx_stopped, rx_stopped) = channel(1);
        let get_workers = (0..num_get_worker)
            .map(|_| GetWorker {
                db: db.clone(),
                rx: rx_get.clone(),
                running: running.clone(),
                _stop_guard: StopGuard(tx_stopped.clone()),
            })
            .collect();
        let write_worker = WriteWorker {
            db,
            rx: rx_write,
            tx: tx_write_done,
            running: running.clone(),
            _stop_guard: StopGuard(tx_stopped),
        };
        Self {
            get_workers,
            write_worker,
            running,
            rx_stopped,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut handles = Vec::new();
        for worker in self.get_workers {
            let handle = std::thread::spawn(move || worker.run());
            handles.push(handle);
        }
        let write_handle = std::thread::spawn(move || self.write_worker.run());
        handles.push(write_handle);

        cancel.run_until_cancelled(self.rx_stopped.recv()).await;
        self.running.store(false, Relaxed);
        for handle in handles {
            let _ = handle.join().unwrap()?;
        }
        Ok(())
    }
}
