use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use loom_contracts::{Backend, Input, ObjectRef, ObjectStore, TaskOutcome, TaskSpec};
use std::{cmp::Reverse, collections::BinaryHeap, io::Read, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(about = "Sort chunks on Loom workers, then merge their immutable results")]
struct Args {
    #[arg(long, env = "LOOM_DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "LOOM_NAMESPACE", default_value = "lab")]
    namespace: String,
    #[arg(long, default_value = "/opt/loom/sort.component.wasm")]
    component: PathBuf,
    /// Generate this many deterministic integers unless --stdin is used.
    #[arg(long, default_value_t = 16000)]
    count: usize,
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..=1000))]
    chunk_size: u32,
    /// Read a JSON array of signed 64-bit integers from standard input.
    #[arg(long)]
    stdin: bool,
    /// Include the complete sorted array in the JSON report.
    #[arg(long)]
    print_result: bool,
    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
}

/// The sorting application only uses backend contracts, never SQL or a DB client.
async fn distributed_sort(
    store: &dyn Backend,
    module: ObjectRef,
    input: &[i64],
    chunk_size: usize,
    run: &str,
) -> Result<(Vec<i64>, Vec<serde_json::Value>)> {
    let mut ids = Vec::new();
    // Submit ALL shards before waiting, so workers can process them concurrently.
    for (index, chunk) in input.chunks(chunk_size).enumerate() {
        let object = store.put(serde_json::to_vec(chunk)?).await?;
        let id = format!("{run}-{index}");
        store
            .submit(TaskSpec {
                id: id.clone(),
                module: module.clone(),
                function: "sort_chunk".into(),
                input: Input::Object(object),
                output: loom_contracts::TaskOutput::Value,
                max_attempts: 3,
                notify: None,
            })
            .await?;
        ids.push(id);
    }
    eprintln!("Submitted {} tasks (run: {run})", ids.len());
    let mut remaining = ids;
    let mut chunks = Vec::new();
    let mut completed = Vec::new();
    while !remaining.is_empty() {
        let mut pending = Vec::new();
        for id in remaining {
            let status = store.task(&id).await?;
            match status.outcome {
                Some(TaskOutcome::Succeeded(object)) => {
                    let chunk: Vec<i64> = serde_json::from_slice(&store.get(&object).await?)?;
                    ensure!(
                        chunk.windows(2).all(|w| w[0] <= w[1]),
                        "unsorted shard: {id}"
                    );
                    completed.push(serde_json::json!({
                        "id": id, "attempts": status.attempts, "output": object.digest,
                    }));
                    chunks.push(chunk);
                }
                Some(TaskOutcome::Failed(error)) => bail!("task {id} failed: {error}"),
                None => pending.push(id),
            }
        }
        remaining = pending;
        if !remaining.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok((merge(chunks), completed))
}

/// O(N log K) k-way merge. It runs in the client, not on a worker.
fn merge(chunks: Vec<Vec<i64>>) -> Vec<i64> {
    let mut heap = BinaryHeap::new();
    let mut output = Vec::with_capacity(chunks.iter().map(Vec::len).sum());
    for (shard, chunk) in chunks.iter().enumerate() {
        if let Some(&value) = chunk.first() {
            heap.push(Reverse((value, shard, 0)));
        }
    }
    while let Some(Reverse((value, shard, index))) = heap.pop() {
        output.push(value);
        if let Some(&next) = chunks[shard].get(index + 1) {
            heap.push(Reverse((next, shard, index + 1)));
        }
    }
    output
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let input: Vec<i64> = if args.stdin {
        let mut json = String::new();
        std::io::stdin().read_to_string(&mut json)?;
        serde_json::from_str(&json).context("stdin must be a JSON array of i64 integers")?
    } else {
        (0..args.count)
            .map(|i| ((i as u64).wrapping_mul(48271) % 100003) as i64 - 50001)
            .collect()
    };
    // This is the composition root: replace the adapter here, not the algorithm.
    let postgres = std::sync::Arc::new(
        loom_backend_postgres::connect(&args.database_url, &args.namespace).await?,
    );
    let store = loom_contracts::Stores::new(postgres.clone(), postgres);
    let module = store.put(std::fs::read(&args.component)?).await?;
    let run = format!("sort-{}", uuid::Uuid::new_v4());
    let (sorted, tasks) = tokio::time::timeout(
        Duration::from_secs(args.timeout_seconds),
        distributed_sort(&store, module, &input, args.chunk_size as usize, &run),
    )
    .await
    .context("timed out; inspect workers and task IDs; submitted tasks are not cancelled")??;

    // Independent local oracle for this tutorial, never a fallback for failed work.
    let mut expected = input;
    expected.sort_unstable();
    ensure!(sorted == expected, "result differs from reference sort");
    let output = store.put(serde_json::to_vec(&sorted)?).await?;
    let mut report = serde_json::json!({
        "run": run, "count": sorted.len(), "task_count": tasks.len(),
        "verified": true, "output": output.digest, "tasks": tasks,
        "first": sorted.first(), "last": sorted.last(),
    });
    if args.print_result {
        report["sorted"] = serde_json::to_value(&sorted)?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
