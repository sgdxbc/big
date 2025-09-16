use std::time::Duration;

use big::{
    logging::init_logging,
    parse::Configs,
    storage::{
        bench::{Bench, BenchPlainStorage},
        plain::PlainStorage,
    },
};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let mut configs = Configs::new();
    configs.parse(
        "
bench.num-key       1000000
bench.put-ratio     0.5
big.prefetch-offset 0
",
    );

    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let db = DB::open_default(temp_dir.path())?;
    PlainStorage::prefill(db, Bench::prefill_items(configs.extract()?)).await?;
    println!("db prefilled");
    let db = DB::open_default(temp_dir.path())?;

    let cancel = CancellationToken::new();
    let bench = BenchPlainStorage::new(configs.extract()?, db.into(), cancel.clone());
    let timeout = async {
        sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(bench.run(), timeout)?;

    temp_dir.close()?;
    Ok(())
}
