use std::time::Duration;

use big::{
    parse::Configs,
    storage::{
        bench::{Bench, BenchStorage},
        plain::PlainStorage,
    },
};
use rand::{SeedableRng as _, rngs::StdRng};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{fs, task::JoinSet, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut configs = Configs::new();
    configs.parse(
        "
bench.num-key   1000000
bench.put-ratio 0.5

big.num-node            1
big.num-faulty-node     0
big.active-push-ahead   0

addrs   127.0.0.1:5000
addrs   127.0.0.1:5001
addrs   127.0.0.1:5002
addrs   127.0.0.1:5003
",
    );

    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let mut dbs = Vec::new();
    for node_index in 0..configs.get("big.num-node")? {
        let db_path = temp_dir.path().join(format!("node-{node_index}"));
        fs::create_dir(&db_path).await?;
        let db = DB::open_default(&db_path)?;
        PlainStorage::prefill(
            &db,
            Bench::prefill_items(configs.extract()?, StdRng::seed_from_u64(117418)),
        )?;
        dbs.push(db);
        println!("db prefilled for node {node_index}")
    }

    let mut addrs = configs.get_values("addrs")?;
    addrs.truncate(configs.get("big.num-node")?);
    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();
    for (node_index, db) in dbs.into_iter().enumerate() {
        let bench = BenchStorage::new(
            node_index as _,
            addrs.clone(),
            configs.extract()?,
            configs.extract()?,
            [node_index as _].into(),
            db.into(),
            (0..configs.get("big.num-node")?).collect(),
            cancel.clone(),
        );
        tasks.spawn(bench.run());
    }
    let bench = async {
        while let Some(result) = tasks.join_next().await {
            result??
        }
        anyhow::Ok(())
    };
    let timeout = async {
        sleep(Duration::from_secs(1)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(bench, timeout)?;

    temp_dir.close()?;
    Ok(())
}
