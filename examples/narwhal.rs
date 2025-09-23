use std::time::Duration;

use big::{logging::init_logging, narwhal::network::Narwhal, parse::Configs};
use tokio::{
    sync::{mpsc::channel, oneshot},
    task::JoinSet,
    time::{sleep, timeout},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let mut configs = Configs::new();
    configs.parse(
        "
narwhal.num-node        4
narwhal.num-faulty-node 1

addrs   127.0.0.1:5000
addrs   127.0.0.1:5001
addrs   127.0.0.1:5002
addrs   127.0.0.1:5003
",
    );

    let mut addrs = configs.get_values("addrs")?;
    addrs.truncate(configs.get("narwhal.num-node")?);
    let cancel = CancellationToken::new();
    let mut tasks = JoinSet::new();
    for node_index in 0..configs.get("narwhal.num-node")? {
        let (tx_block, mut rx_block) = channel(100);
        let narwhal = Narwhal::new(
            configs.extract()?,
            node_index,
            true,
            (0..configs.get("narwhal.num-node")?).collect(),
            addrs.clone(),
            node_index as _,
            channel(1).1,
            tx_block,
            oneshot::channel().0,
        );
        tasks.spawn(narwhal.run(cancel.clone()));
        tasks.spawn(async move {
            loop {
                let mut num_interval_block = 0;
                let interval = async {
                    while let Some(_block) = rx_block.recv().await {
                        num_interval_block += 1;
                    }
                };
                match timeout(Duration::from_secs(1), interval).await {
                    Err(_) => {
                        info!("[{node_index}] {num_interval_block} blocks/sec")
                    }
                    Ok(()) => break,
                }
            }
            Ok(())
        });
    }
    let bench = async {
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        anyhow::Ok(())
    };
    let timeout = async {
        sleep(Duration::from_secs(10)).await;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(bench, timeout)?;
    Ok(())
}
