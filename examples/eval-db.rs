#![allow(unused)]
use std::{collections::HashMap, path::PathBuf, thread::available_parallelism, time::Duration};

use big::{
    logging::init_logging,
    parse::Configs,
    storage::{StorageKey, StorageOp, bench::Bench},
};
use bytes::Bytes;
use rocksdb::{DB, Options, WriteBatch};
use tempfile::tempdir;
use tokio::{
    sync::mpsc::{Receiver, channel},
    time::sleep,
    try_join,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let mut configs = Configs::new();
    configs.parse(
        "
bench.num-key           10000000
bench.put-ratio         0.
bench.prefetch-offset   10
    ",
    );

    let (tx_op, rx_op) = channel(100);
    let bench = Bench::new(configs.extract()?, tx_op);

    // let mut storage = Backend::InMemory(InMemory::new(rx_op));
    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let mut storage = Backend::Inline(Inline::new(temp_dir.path().to_owned(), rx_op));

    storage.prefill(Bench::prefill_items(configs.extract()?))?;

    let cancel = CancellationToken::new();
    let timeout = async {
        sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(
        timeout,
        bench.run(cancel.clone()),
        storage.run(cancel.clone())
    )?;
    temp_dir.close()?;
    Ok(())
}

enum Backend {
    InMemory(InMemory),
    Inline(Inline),
}

impl Backend {
    fn prefill(
        &mut self,
        items: impl IntoIterator<Item = (StorageKey, Bytes)>,
    ) -> anyhow::Result<()> {
        match self {
            Backend::InMemory(s) => s.prefill(items),
            Backend::Inline(s) => s.prefill(items)?,
        }
        Ok(())
    }

    async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        match self {
            Backend::InMemory(s) => s.run(cancel).await,
            Backend::Inline(s) => s.run(cancel).await,
        }
    }
}

struct InMemory {
    rx_op: Receiver<StorageOp>,

    store: HashMap<StorageKey, Bytes>,
}

impl InMemory {
    fn new(rx_op: Receiver<StorageOp>) -> Self {
        Self {
            rx_op,
            store: Default::default(),
        }
    }

    fn prefill(&mut self, items: impl IntoIterator<Item = (StorageKey, Bytes)>) {
        for (k, v) in items {
            self.store.insert(k, v);
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        while let Some(Some(op)) = cancel.run_until_cancelled(self.rx_op.recv()).await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let v = self.store.get(&key).cloned();
                    let _ = tx_value.send(v);
                }
                StorageOp::Bump(updates, tx_ok) => {
                    for (key, value) in updates {
                        match value {
                            None => self.store.remove(&key),
                            Some(value) => self.store.insert(key, value),
                        };
                    }
                    let _ = tx_ok.send(());
                }
                StorageOp::Prefetch(..) => {} // no-op
                StorageOp::VoteArchive(..) => unimplemented!(),
            }
        }
        Ok(())
    }
}

struct Inline {
    path: PathBuf,

    rx_op: Receiver<StorageOp>,
}

impl Inline {
    fn new(path: PathBuf, rx_op: Receiver<StorageOp>) -> Self {
        Self { path, rx_op }
    }

    fn prefill(&self, items: impl IntoIterator<Item = (StorageKey, Bytes)>) -> anyhow::Result<()> {
        common_prefill(items.into_iter(), &self.path)
    }

    async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let db = DB::open_default(self.path)?;
        while let Some(Some(op)) = cancel.run_until_cancelled(self.rx_op.recv()).await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let v = db.get(key)?;
                    let _ = tx_value.send(v.map(Into::into));
                }
                StorageOp::Bump(updates, tx_ok) => {
                    let mut batch = WriteBatch::default();
                    for (key, value) in updates {
                        match value {
                            None => batch.delete(key),
                            Some(value) => batch.put(key, value),
                        };
                    }
                    db.write(batch)?;
                    let _ = tx_ok.send(());
                }
                StorageOp::Prefetch(..) => {} // no-op
                StorageOp::VoteArchive(..) => unimplemented!(),
            }
        }
        Ok(())
    }
}

fn common_prefill(
    mut items: impl Iterator<Item = (StorageKey, Bytes)>,
    path: &PathBuf,
) -> Result<(), anyhow::Error> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.prepare_for_bulk_load();
    options.set_max_subcompactions(available_parallelism()?.get() as _);
    let db = DB::open(&options, path)?;

    let mut batch;
    while {
        batch = WriteBatch::default();
        for (k, v) in &mut items {
            batch.put(k.as_ref(), &v);
            if batch.len() == 1000 {
                break;
            }
        }
        !batch.is_empty()
    } {
        db.write(batch)?
    }
    db.compact_range::<&[u8], &[u8]>(None, None);
    Ok(())
}
