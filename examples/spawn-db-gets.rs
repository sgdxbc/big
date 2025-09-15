use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rand::{Rng as _, SeedableRng, rngs::StdRng};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{
    spawn,
    sync::oneshot,
    task::{JoinSet, yield_now},
};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Inline,
    Spawn,
    SpawnSnapshot,
}

const NUM_KEY: u64 = 1_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let db = DB::open_default(temp_dir.path())?;
    let mut value = [0; 68];
    let mut rng = rand::rng();
    for index in 0..NUM_KEY {
        let key = uniform_key(index);
        rng.fill(&mut value);
        db.put(key, &value)?
    }
    let db = Arc::new(db);
    let writes = spawn({
        let db = db.clone();
        async move {
            let mut rng = StdRng::from_rng(&mut rand::rng());
            let mut value = [0; 68];
            loop {
                let key = uniform_key(rng.random_range(0..NUM_KEY));
                rng.fill(&mut value);
                db.put(key, &value)?;
                yield_now().await
            }
            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    });

    eprintln!("mode,task_size,concurrency,num_task,total_secs,mean_latency_secs");
    for task_size in [1, 5, 10, 15] {
        bench(Mode::Inline, task_size, 1, db.clone()).await?;
        for concurrency in [1, 2, 3, 4, 8, 16] {
            bench(Mode::Spawn, task_size, concurrency, db.clone()).await?;
            bench(Mode::SpawnSnapshot, task_size, concurrency, db.clone()).await?;
        }
    }

    writes.abort();
    match writes.await {
        Ok(_) => unreachable!(),
        Err(err) => anyhow::ensure!(err.is_cancelled(), "writer task failed: {err}"),
    }
    Ok(())
}

async fn bench(
    mode: Mode,
    task_size: usize,
    concurrency: usize,
    db: Arc<DB>,
) -> anyhow::Result<()> {
    let num_task = 1_000_000 / task_size;

    println!("mode: {mode:?}, task size: {task_size}, concurrency: {concurrency}");
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(num_task);

    let task = |start: Instant| {
        let db = db.clone();
        async move {
            let mut rng = StdRng::from_rng(&mut rand::rng());
            for _ in 0..task_size {
                let key = uniform_key(rng.random_range(0..NUM_KEY));
                let Some(_value) = db.get(key)? else {
                    anyhow::bail!("missing key")
                };
            }
            Ok(start)
        }
    };
    match mode {
        Mode::Inline => {
            for _ in 0..num_task {
                latencies.push(task(Instant::now()).await?.elapsed())
            }
        }
        Mode::Spawn => {
            let mut tasks = JoinSet::new();
            for _ in 0..concurrency {
                tasks.spawn(task(Instant::now()));
            }
            for _ in 0..num_task - concurrency {
                let Some(res) = tasks.join_next().await else {
                    unreachable!()
                };
                latencies.push(res??.elapsed());
                tasks.spawn(task(Instant::now()));
            }
            while let Some(res) = tasks.join_next().await {
                latencies.push(res??.elapsed())
            }
        }
        Mode::SpawnSnapshot => {
            let task = |start: Instant, tx_ready: oneshot::Sender<()>| {
                let db = db.clone();
                async move {
                    let snapshot = db.snapshot();
                    let _ = tx_ready.send(());
                    let mut rng = StdRng::from_rng(&mut rand::rng());
                    for _ in 0..task_size {
                        let key = uniform_key(rng.random_range(0..NUM_KEY));
                        let Some(_value) = snapshot.get(key)? else {
                            anyhow::bail!("missing key")
                        };
                    }
                    Ok(start)
                }
            };
            let mut tasks = JoinSet::new();
            for _ in 0..concurrency {
                let (tx_ready, rx_ready) = oneshot::channel();
                tasks.spawn(task(Instant::now(), tx_ready));
                rx_ready.await?
            }
            for _ in 0..num_task - concurrency {
                let Some(res) = tasks.join_next().await else {
                    unreachable!()
                };
                latencies.push(res??.elapsed());
                let (tx_ready, rx_ready) = oneshot::channel();
                tasks.spawn(task(Instant::now(), tx_ready));
                rx_ready.await?
            }
            while let Some(res) = tasks.join_next().await {
                latencies.push(res??.elapsed())
            }
        }
    }

    let elapsed = start.elapsed();
    let ops_per_sec = num_task as f64 / elapsed.as_secs_f64();
    let mean_latency = latencies.iter().sum::<Duration>() / num_task as u32;
    println!(
        "ops: {num_task}, total: {elapsed:.1?}, ops/sec: {ops_per_sec:.1}, mean latency: {mean_latency:.1?}",
    );
    eprintln!(
        "{mode:?},{task_size},{concurrency},{num_task},{},{}",
        elapsed.as_secs_f64(),
        mean_latency.as_secs_f64()
    );
    Ok(())
}

fn uniform_key(index: u64) -> [u8; 32] {
    StdRng::seed_from_u64(index).random()
}
