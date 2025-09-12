use std::time::Duration;

use big::{
    parse::Configs,
    storage::{
        bench::{Bench, BenchPlain},
        plain::Plain,
    },
};
use rand::{SeedableRng as _, rngs::StdRng};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut configs = Configs::new();
    configs.parse(
        "
bench.num-key   1000000
bench.put-ratio 0.5
",
    );

    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let db = DB::open_default(temp_dir.path())?;
    Plain::prefill(
        &db,
        Bench::prefill_items(configs.extract()?, StdRng::seed_from_u64(117418)),
    )?;
    println!("db prefilled");

    let cancel = CancellationToken::new();
    let mut bench = BenchPlain::new(configs.extract()?, db.into(), cancel.clone());
    let timeout = async {
        sleep(Duration::from_secs(1)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(bench.run(), timeout)?;

    temp_dir.close()?;
    Ok(())
}
