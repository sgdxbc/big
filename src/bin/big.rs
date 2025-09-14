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
use rocksdb::DB;
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
    let mut db = DB::open_default(temp_dir.path())?;
    let items = Bench::prefill_items(configs.extract()?);
    let cancel = CancellationToken::new();
    let timeout = {
        let cancel = cancel.clone();
        async move {
            sleep(Duration::from_secs(10)).await;
            cancel.cancel();
            anyhow::Ok(())
        }
    };
    if configs.get("big.plain-storage")? {
        PlainStorage::prefill(&db, items)?;
        let bench = BenchPlainStorage::new(configs.extract()?, db.into(), cancel);
        try_join!(bench.run(), timeout)?;
    } else {
        StorageCore::prefill(&mut db, items, &configs.extract()?, [index].into())?;
        let mut addrs = configs.get_values("addrs")?;
        addrs.truncate(configs.get("big.num-node")?);
        let bench = BenchStorage::new(
            index as _,
            addrs,
            configs.extract()?,
            configs.extract()?,
            [index].into(),
            db.into(),
            (0..configs.get("big.num-node")?).collect(),
            cancel,
        );
        try_join!(bench.run(), timeout)?;
    }
    Ok(())
}
