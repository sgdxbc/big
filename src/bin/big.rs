use std::{env::args, fs, path::Path, time::Duration};

use big::{
    logging::init_logging_file,
    parse::Configs,
    storage::{
        StorageConfig, StorageCore,
        bench::{Bench, BenchPlainStorage, BenchStorage},
        plain::PlainStorage,
    },
};
use rocksdb::{DB, Options};
use tempfile::TempDir;
use tokio::{process::Command, sync::oneshot, time::sleep, try_join};
use tokio_util::sync::CancellationToken;
use tracing::info;

fn main() -> anyhow::Result<()> {
    init_logging_file("big.log")?;
    let Some(role) = args().nth(1) else {
        anyhow::bail!("missing role argument")
    };
    let mut configs = Configs::new();
    for section in ["task", "addr"] {
        configs.parse(&fs::read_to_string(format!("big-configs/{section}.conf"))?);
        if let Ok(s) = fs::read_to_string(format!("big-configs/{section}.override.conf")) {
            info!("overriding configs from {section}.override.conf");
            configs.parse(&s)
        }
    }

    if let Some(index) = role.strip_prefix("bench") {
        role_bench(configs, index.parse()?)
    } else if let Some(index) = role.strip_prefix("prefill") {
        role_prefill(configs, index.parse()?)
    } else {
        anyhow::bail!("unknown role: {role}")
    }
}

#[tokio::main]
async fn role_prefill(configs: Configs, index: u16) -> anyhow::Result<()> {
    if index >= configs.get("big.num-node")? {
        return Ok(());
    }

    let path = Path::new("big-db-prefill");
    let _ = fs::remove_dir_all(path);
    fs::create_dir(path)?;
    let mut options = Options::default();
    options.prepare_for_bulk_load();
    options.create_if_missing(true);
    let mut db = DB::open(&options, path)?;
    let items = Bench::prefill_items(configs.extract()?);
    if configs.get("big.plain-storage")? {
        PlainStorage::prefill(db, items).await
    } else {
        StorageCore::prefill(&mut db, items, &configs.extract()?, [index].into())
    }
}

#[tokio::main]
async fn role_bench(configs: Configs, index: u16) -> anyhow::Result<()> {
    if index >= configs.get("big.num-node")? {
        return Ok(());
    }

    let temp_dir = TempDir::with_prefix("big-db.")?;
    let prefill_path = Path::new("big-db-prefill");
    if prefill_path.exists() {
        info!("copying prefill db");
        let status = Command::new("cp")
            .arg("-rT")
            .arg(prefill_path)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success());
        info!("prefill done")
    }

    let cancel = CancellationToken::new();
    let (tx_start, rx_start) = oneshot::channel();
    let timeout = {
        let cancel = cancel.clone();
        async move {
            let _ = rx_start.await;
            sleep(Duration::from_secs(10)).await;
            cancel.cancel();
            anyhow::Ok(())
        }
    };
    if configs.get("big.plain-storage")? {
        let db = open_db(temp_dir.path(), (0x00..=0xff).map(|i| format!("{i:02x}")))?;
        let bench = BenchPlainStorage::new(configs.extract()?, db.into(), cancel);
        let _ = tx_start.send(());
        try_join!(bench.run(), timeout)?;
    } else {
        let storage_config = configs.extract::<StorageConfig>()?;
        let node_indices = vec![index];
        let cfs = storage_config
            .segments_of(&node_indices)
            .map(|i| format!("segment-{i}"));
        let db = open_db(temp_dir.path(), cfs)?;

        let mut addrs = configs.get_values("addrs")?;
        addrs.truncate(configs.get("big.num-node")?);
        let bench = BenchStorage::new(
            index as _,
            addrs,
            configs.extract()?,
            storage_config,
            node_indices,
            db.into(),
            (0..configs.get("big.num-node")?).collect(),
            cancel,
            tx_start,
        );
        try_join!(bench.run(), timeout)?;
    }

    Ok(())
}

fn open_db(path: impl AsRef<Path>, cfs: impl Iterator<Item = String>) -> anyhow::Result<DB> {
    let db = DB::open_cf(&Default::default(), path, cfs)?;
    db.put("", "")?;
    db.get("")?;
    info!("db ready");
    Ok(db)
}
