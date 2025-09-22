use std::{env::args, fs, path::Path, sync::Arc, time::Duration};

use big::{
    logging::init_logging_file,
    parse::Configs,
    storage::{
        Storage,
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

    let path = Path::new("/tmp/big-db-prefill");
    let _ = fs::remove_dir_all(path);
    fs::create_dir(path)?;
    let mut options = Options::default();
    options.prepare_for_bulk_load();
    options.create_if_missing(true);
    options.increase_parallelism(std::thread::available_parallelism()?.get() as _);
    options.set_max_subcompactions(std::thread::available_parallelism()?.get() as _);
    let db = DB::open(&options, path)?;
    let items = Bench::prefill_items(configs.extract()?);
    if configs.get("big.plain-storage")? {
        PlainStorage::prefill(db, items).await
    } else {
        Storage::prefill(db, items, &configs.extract()?, [index].into()).await
    }
}

#[tokio::main]
async fn role_bench(configs: Configs, index: u16) -> anyhow::Result<()> {
    if index >= configs.get("big.num-node")? {
        return Ok(());
    }

    let temp_dir = TempDir::with_prefix("big-db.")?;
    let prefill_path = Path::new("/tmp/big-db-prefill");
    info!("copying prefill db");
    let status = Command::new("cp")
        .arg("-rT")
        .arg(prefill_path)
        .arg(temp_dir.path())
        .status()
        .await?;
    anyhow::ensure!(status.success());
    info!("prefill done");

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

    let mut options = Options::default();
    // let (mut options, cf_descs) = Options::load_latest(
    //     temp_dir.path(),
    //     rocksdb::Env::new()?,
    //     false,
    //     rocksdb::Cache::new_lru_cache(512 << 20),
    // )?;
    options.enable_statistics();
    options.increase_parallelism(std::thread::available_parallelism()?.get() as _);
    let db = Arc::new(DB::open(&options, temp_dir.path())?);
    if configs.get("big.plain-storage")? {
        // db = Arc::new(DB::open_cf_descriptors(
        //     &options,
        //     temp_dir.path(),
        //     cf_descs,
        // )?);
        let bench = BenchPlainStorage::new(true, configs.extract()?, db.clone());
        let _ = tx_start.send(());
        try_join!(bench.run(cancel), timeout)?;
    } else {
        let mut addrs = configs.get_values("addrs")?;
        addrs.truncate(configs.get("big.num-node")?);
        let bench = BenchStorage::new(
            index as _,
            addrs,
            configs.extract()?,
            configs.extract()?,
            vec![index],
            db.clone(),
            (0..configs.get("big.num-node")?).collect(),
            tx_start,
        );
        try_join!(bench.run(cancel), timeout)?;
    }
    if let Some(stats) = db.property_value("rocksdb.stats")? {
        info!("rocksdb.stats\n{stats}")
    }
    if let Some(stats) = db.property_value("rocksdb.options-statistics")? {
        info!("rocksdb.options-statistics\n{stats}")
    }
    // if let Some(stats) = db.property_value("rocksdb.sstables")? {
    //     info!("rocksdb.sstables\n{stats}")
    // }

    Ok(())
}
