use std::{env::args, fs, time::Duration};

use big::{
    logging::init_logging_file,
    parse::Configs,
    storage::{
        StorageCore,
        bench::{Bench, BenchPlainStorage, BenchStorage},
        plain::PlainStorage,
    },
};
use rocksdb::{DB, Options};
use tempfile::TempDir;
use tokio::{time::sleep, try_join};
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
    } else {
        anyhow::bail!("unknown role: {role}")
    }
}

#[tokio::main]
async fn role_bench(configs: Configs, index: u16) -> anyhow::Result<()> {
    if index >= configs.get("big.num-node")? {
        return Ok(());
    }

    let temp_dir = TempDir::with_prefix("big-db.")?;
    let mut options = Options::default();
    options.prepare_for_bulk_load();
    options.create_if_missing(true);
    let mut db = DB::open(&options, temp_dir.path())?;
    let items = Bench::prefill_items(configs.extract()?);
    if configs.get("big.plain-storage")? {
        PlainStorage::prefill(&db, items)?;
        info!("db prefilled");
        drop(db);
        info!("db start reopening");
        db = DB::open_default(temp_dir.path())?;
        info!("db reopened")
    } else {
        let cfs = StorageCore::prefill(&mut db, items, &configs.extract()?, [index].into())?;
        info!("db prefilled");
        drop(db);
        info!("db start reopening");
        db = DB::open_cf(&Default::default(), temp_dir.path(), cfs)?;
        info!("db reopened")
    }
    db.get("")?;
    db.put("", "")?;
    info!("db ready");
    let db = db.into();

    let cancel = CancellationToken::new();
    let timeout = {
        let cancel = cancel.clone();
        async move {
            sleep(Duration::from_secs(20)).await;
            cancel.cancel();
            anyhow::Ok(())
        }
    };
    if configs.get("big.plain-storage")? {
        let bench = BenchPlainStorage::new(configs.extract()?, db, cancel);
        try_join!(bench.run(), timeout)?;
    } else {
        let mut addrs = configs.get_values("addrs")?;
        addrs.truncate(configs.get("big.num-node")?);
        let bench = BenchStorage::new(
            index as _,
            addrs,
            configs.extract()?,
            configs.extract()?,
            [index].into(),
            db,
            (0..configs.get("big.num-node")?).collect(),
            cancel,
        );
        try_join!(bench.run(), timeout)?;
    }

    Ok(())
}
