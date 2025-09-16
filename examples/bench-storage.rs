use std::{sync::Arc, time::Duration};

use big::{
    logging::init_logging,
    parse::Configs,
    storage::{
        StorageCore,
        bench::{Bench, BenchStorage},
    },
};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{fs, sync::oneshot, task::JoinSet, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let mut configs = Configs::new();
    configs.parse(
        "
bench.num-key   1000000
bench.put-ratio 0.5

big.num-node            4
big.num-faulty-node     1
big.num-stripe          10
big.num-backup          1
big.prefetch-offset     10

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
        let mut db = DB::open_default(&db_path)?;
        StorageCore::prefill(
            &mut db,
            Bench::prefill_items(configs.extract()?),
            &configs.extract()?,
            [node_index as _].into(),
        )?;
        println!("db prefilled for node {node_index}");
        dbs.push(Arc::new(db))
    }

    let mut addrs = configs.get_values("addrs")?;
    addrs.truncate(configs.get("big.num-node")?);
    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();
    for (node_index, db) in dbs.iter().enumerate() {
        let bench = BenchStorage::new(
            node_index as _,
            addrs.clone(),
            configs.extract()?,
            configs.extract()?,
            [node_index as _].into(),
            db.clone(),
            (0..configs.get("big.num-node")?).collect(),
            cancel.clone(),
            oneshot::channel().0, // omit observing establishment for local testing
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
        sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(bench, timeout)?;
    let sizes = dbs
        .iter()
        .map(|db| db.property_int_value("rocksdb.estimate-live-data-size"))
        .collect::<Result<Vec<_>, _>>()?;
    println!("data sizes: {:?}", sizes);

    temp_dir.close()?;
    Ok(())
}
