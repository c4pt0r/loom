use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use loom_contracts::{Backend, BlobStore, Input, ObjectRef, TaskOutcome, TaskSpec};
use std::{collections::BTreeMap, io::Read, path::PathBuf, time::Duration};

const MAX_CHUNK_BYTES: usize = 16 * 1024;
type Counts = BTreeMap<String, u64>;

#[derive(Parser)]
#[command(about = "Count whitespace-separated words on Loom workers and merge frequencies")]
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

/// Application orchestration depends on contracts; SQL is confined to the adapter.
async fn word_count(
    store: &dyn Backend,
    module: ObjectRef,
    chunks: &[String],
    run: &str,
) -> Result<(Counts, Vec<serde_json::Value>)> {
    let mut remaining = Vec::new();
    for (index, chunk) in chunks.iter().enumerate() {
        let object = store.put(serde_json::to_vec(chunk)?).await?;
        let id = format!("{run}-{index}");
        store
            .submit(TaskSpec {
                id: id.clone(),
                module: module.clone(),
                function: "count_words".into(),
                input: Input::Object(object),
                max_attempts: 3,
                notify: None,
            })
            .await?;
        remaining.push(id);
    }
    eprintln!("Submitted {} tasks (run: {run})", remaining.len());
    let mut totals = Counts::new();
    let mut completed = Vec::new();
    while !remaining.is_empty() {
        let mut pending = Vec::new();
        for id in remaining {
            let status = store.task(&id).await?;
            match status.outcome {
                Some(TaskOutcome::Succeeded(object)) => {
                    let partial: Counts = serde_json::from_slice(&store.get(&object).await?)?;
                    for (word, count) in partial {
                        let total = totals.entry(word).or_default();
                        *total = total.checked_add(count).context("word count overflow")?;
                    }
                    completed.push(serde_json::json!({
                        "id": id, "attempts": status.attempts, "output": object.digest,
                    }));
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
    Ok((totals, completed))
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
    let expected_total = text.split_whitespace().count() as u64;
    let store = loom_backend_postgres::connect(&args.database_url, &args.namespace).await?;
    let module = store.put(std::fs::read(&args.component)?).await?;
    let run = format!("word-count-{}", uuid::Uuid::new_v4());
    let (counts, tasks) = tokio::time::timeout(
        Duration::from_secs(args.timeout_seconds),
        word_count(&store, module, &chunks, &run),
    )
    .await
    .context("timed out; submitted tasks are not cancelled")??;
    let total_words = counts.values().try_fold(0_u64, |sum, count| {
        sum.checked_add(*count).context("total word count overflow")
    })?;
    ensure!(
        total_words == expected_total,
        "word total differs from input"
    );
    let output = store.put(serde_json::to_vec(&counts)?).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "run": run, "total_words": total_words, "unique_words": counts.len(),
            "task_count": tasks.len(), "counts": counts, "output": output.digest,
            "tasks": tasks,
        }))?
    );
    Ok(())
}
