use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use loom_contracts::{
    Backend, BlobStore, Input, MAX_SHUFFLE_SOURCES, ObjectRef, PartitionedDataset, ShuffleLayout,
    ShuffleSource, TaskOutcome, TaskOutput, TaskSpec,
};
use std::{collections::BTreeMap, io::Read, path::PathBuf, time::Duration};

const MAX_CHUNK_BYTES: usize = 16 * 1024;
type Counts = BTreeMap<String, u64>;

#[derive(Parser)]
#[command(about = "Count words using distributed maps, hash shuffle, and partition reducers")]
struct Args {
    #[arg(long, env = "LOOM_DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "LOOM_NAMESPACE", default_value = "lab")]
    namespace: String,
    #[arg(long, default_value = "/opt/loom/word-count.component.wasm")]
    component: PathBuf,
    /// Read plain UTF-8 text instead of the bundled 16,000-word sample.
    #[arg(long)]
    stdin: bool,
    /// Maximum words per task. Chunks also have a 16 KiB UTF-8 byte limit.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..=1000))]
    chunk_words: u32,
    /// Number of independently reduced hash partitions.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..=256))]
    reducers: u32,
    /// Download final partitions for display; never downloads intermediate map results.
    #[arg(long)]
    print_result: bool,
    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
}

/// Split only between words, never at an arbitrary byte or character offset.
fn split_chunks(text: &str, chunk_words: usize) -> Result<Vec<String>> {
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut words = 0;
    for word in text.split_whitespace() {
        ensure!(
            word.len() <= MAX_CHUNK_BYTES,
            "a word exceeds the 16 KiB limit"
        );
        if !chunk.is_empty()
            && (words == chunk_words || chunk.len() + 1 + word.len() > MAX_CHUNK_BYTES)
        {
            chunks.push(std::mem::take(&mut chunk));
            words = 0;
        }
        if !chunk.is_empty() {
            chunk.push(' ');
        }
        chunk.push_str(word);
        words += 1;
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    Ok(chunks)
}

/// Submit the graph without loading intermediate data or performing reduction.
async fn word_count(
    store: &dyn Backend,
    module: ObjectRef,
    chunks: &[String],
    layout: &ShuffleLayout,
    run: &str,
) -> Result<(
    PartitionedDataset,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
)> {
    let mut map_ids = Vec::new();
    for (index, chunk) in chunks.iter().enumerate() {
        let object = store.put(serde_json::to_vec(chunk)?).await?;
        let id = format!("{run}-map-{index}");
        store
            .submit(TaskSpec {
                id: id.clone(),
                module: module.clone(),
                function: "count_words".into(),
                input: Input::Object(object),
                output: TaskOutput::Shuffle(layout.clone()),
                max_attempts: 3,
                notify: None,
            })
            .await?;
        map_ids.push(id);
    }
    let sources = map_ids
        .iter()
        .cloned()
        .map(ShuffleSource::TaskResult)
        .collect::<Vec<_>>();
    let mut reduce_ids = Vec::new();
    for partition in 0..layout.partitions {
        let id = format!("{run}-reduce-{partition}");
        store
            .submit(TaskSpec {
                id: id.clone(),
                module: module.clone(),
                function: "sum_counts".into(),
                input: Input::Shuffle {
                    sources: sources.clone(),
                    layout: layout.clone(),
                    partition,
                },
                output: TaskOutput::Value,
                max_attempts: 3,
                notify: None,
            })
            .await?;
        reduce_ids.push(id);
    }
    eprintln!(
        "Submitted {} maps + {} reducers (run: {run})",
        map_ids.len(),
        reduce_ids.len()
    );
    let mut outputs = vec![None; reduce_ids.len()];
    while outputs.iter().any(Option::is_none) {
        for (partition, id) in reduce_ids.iter().enumerate() {
            if outputs[partition].is_some() {
                continue;
            }
            match store.task(id).await?.outcome {
                Some(TaskOutcome::Succeeded(reference)) => outputs[partition] = Some(reference),
                Some(TaskOutcome::Failed(error)) => bail!("reducer {id} failed: {error}"),
                None => (),
            }
        }
        if outputs.iter().any(Option::is_none) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let mut maps = Vec::new();
    let mut reducers = Vec::new();
    for (ids, reports) in [(&map_ids, &mut maps), (&reduce_ids, &mut reducers)] {
        for id in ids {
            let task = store.task(id).await?;
            let Some(TaskOutcome::Succeeded(output)) = task.outcome else {
                bail!("task {id} did not succeed");
            };
            reports.push(
                serde_json::json!({"id": id, "attempts": task.attempts, "output": output.digest}),
            );
        }
    }
    Ok((
        PartitionedDataset {
            version: 1,
            layout: layout.clone(),
            partitions: outputs.into_iter().map(Option::unwrap).collect(),
        },
        maps,
        reducers,
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let text = if args.stdin {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .context("stdin must be valid UTF-8")?;
        text
    } else {
        "hello loom hello wasm\n".repeat(4000)
    };
    // Validate and split before submitting any work.
    let chunks = split_chunks(&text, args.chunk_words as usize)?;
    ensure!(
        chunks.len() <= MAX_SHUFFLE_SOURCES,
        "example supports at most 1024 map chunks"
    );
    let expected_total = text.split_whitespace().count() as u64;
    let layout = ShuffleLayout {
        partitions: args.reducers,
    };
    let store = loom_backend_postgres::connect(&args.database_url, &args.namespace).await?;
    let module = store.put(std::fs::read(&args.component)?).await?;
    let run = format!("word-count-{}", uuid::Uuid::new_v4());
    let (dataset, maps, reducers) = tokio::time::timeout(
        Duration::from_secs(args.timeout_seconds),
        word_count(&store, module, &chunks, &layout, &run),
    )
    .await
    .context("timed out; submitted tasks are not cancelled")??;
    let output = store.put(serde_json::to_vec(&dataset)?).await?;
    let mut report = serde_json::json!({
        "run": run, "total_words": expected_total,
        "map_task_count": maps.len(), "reduce_task_count": reducers.len(),
        "task_count": maps.len() + reducers.len(), "output": output.digest,
        "partitions": dataset.partitions, "map_tasks": maps, "reduce_tasks": reducers,
    });
    if args.print_result {
        // Optional presentation only: concatenate disjoint FINAL key maps, never sum them.
        let mut counts = Counts::new();
        for reference in &dataset.partitions {
            let part: Counts = serde_json::from_slice(&store.get(reference).await?)?;
            for (word, count) in part {
                ensure!(
                    counts.insert(word, count).is_none(),
                    "key appears in multiple final partitions"
                );
            }
        }
        let total = counts.values().try_fold(0_u64, |sum, n| {
            sum.checked_add(*n).context("total overflow")
        })?;
        ensure!(total == expected_total, "word total differs from input");
        report["unique_words"] = counts.len().into();
        report["counts"] = serde_json::to_value(counts)?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
