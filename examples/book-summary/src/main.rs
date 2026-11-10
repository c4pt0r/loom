use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use loom_book_summary::{pi::LocalPi, *};
use loom_contracts::{ExecutionStore, NodeSession, ObjectStore};
use loom_runtime::Node;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(about = "Summarize chapters using durable Loom tasks and your LOCAL Pi executable")]
struct Args {
    #[arg(long, env = "LOOM_DATABASE_URL")]
    database_url: String,
    /// Use a dedicated namespace: stock Wasm workers cannot execute native Pi jobs.
    #[arg(long, default_value = "book-summary")]
    namespace: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(clap::Args)]
struct Worker {
    /// Absolute path to a local pi installation. Defaults to `pi` resolved on PATH.
    #[arg(long, env = "LOOM_PI_BIN", default_value = "pi")]
    pi: PathBuf,
    #[arg(long, default_value = "work/book-summary")]
    work_dir: PathBuf,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=16))]
    slots: u16,
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pi_timeout_seconds: u64,
}
#[derive(Subcommand)]
enum Command {
    /// Submit a book. Starts no agents; run a native worker separately.
    Submit {
        #[arg(long)]
        id: String,
        #[arg(long)]
        title: String,
        /// Chapter files in book order; uploaded to the object store.
        #[arg(
            long,
            required_unless_present = "manifest",
            conflicts_with = "manifest"
        )]
        chapter: Vec<PathBuf>,
        /// JSON array of Chapter entries, supporting Inline, Object, and ObjectSlice sources.
        #[arg(long)]
        manifest: Option<PathBuf>,
        #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=16))]
        concurrency: u16,
        /// Total attempts. Retries may repeat billable model calls.
        #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(1..=3))]
        attempts: u32,
        /// Optional Pi provider/model identifier; otherwise use local Pi's configured default.
        #[arg(long)]
        model: Option<String>,
    },
    /// Run on the host where Pi is installed. Ctrl-C drains active work.
    Worker(Worker),
    /// Show persisted progress and optionally save a completed report.
    Status {
        #[arg(long)]
        id: String,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Wait for completion; no execution is performed by this command.
        #[arg(long, default_value_t = 0)]
        wait_seconds: u64,
    },
}

fn resolve_pi(path: &std::path::Path) -> Result<PathBuf> {
    let resolved = if path.is_absolute() || path.components().count() > 1 {
        path.to_path_buf()
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|p| p.join(path))
            .find(|p| p.is_file())
            .context("local pi not found on PATH; pass --pi /absolute/path/to/pi")?
    };
    std::fs::canonicalize(resolved).context("cannot resolve local Pi executable")
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let store =
        Arc::new(loom_backend_postgres::connect(&args.database_url, &args.namespace).await?);
    match args.command {
        Command::Submit {
            id,
            title,
            chapter,
            manifest,
            concurrency,
            attempts,
            model,
        } => {
            let chapters = if let Some(path) = manifest {
                serde_json::from_slice(&std::fs::read(path)?)?
            } else {
                let mut chapters = Vec::new();
                for (i, path) in chapter.iter().enumerate() {
                    let text = std::fs::read_to_string(path)?;
                    ensure!(
                        !text.trim().is_empty() && text.len() <= MAX_SOURCE_BYTES,
                        "chapter must be nonempty and <=256 KiB"
                    );
                    let source = Source::Object(store.put(text.into_bytes()).await?);
                    chapters.push(Chapter {
                        id: format!("chapter-{:02}", i + 1),
                        title: path
                            .file_stem()
                            .context("chapter has no filename")?
                            .to_string_lossy()
                            .into_owned(),
                        source,
                    });
                }
                chapters
            };
            submit(
                store.as_ref(),
                &id,
                Book {
                    title,
                    chapters,
                    concurrency: concurrency.into(),
                    max_attempts: attempts,
                    model,
                },
            )
            .await?;
            println!(
                "{}",
                serde_json::json!({"actor":id,"namespace":args.namespace,"submitted":true})
            );
        }
        Command::Worker(w) => {
            let executable = resolve_pi(&w.pi)?;
            std::fs::create_dir_all(&w.work_dir)?;
            let work_dir = std::fs::canonicalize(&w.work_dir)?;
            let module = store.put(MODULE.to_vec()).await?;
            let engine = Arc::new(BookEngine {
                objects: store.clone(),
                module,
                executor: Arc::new(LocalPi {
                    executable: executable.clone(),
                    work_dir,
                    timeout: Duration::from_secs(w.pi_timeout_seconds),
                }),
            });
            eprintln!(
                "LOCAL_PI {} namespace={} slots={}",
                executable.display(),
                args.namespace,
                w.slots
            );
            Node {
                store,
                engine,
                session: NodeSession {
                    node: format!("pi-{}", uuid::Uuid::new_v4()),
                    incarnation: uuid::Uuid::new_v4().to_string(),
                },
                lease_ms: 5000,
            }
            .run_with_slots(w.slots.into(), async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        }
        Command::Status {
            id,
            output,
            wait_seconds,
        } => {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);
            loop {
                let snapshot = store.actor(&id).await?;
                let state: State = decode(&snapshot.state)?;
                if state.phase == "completed"
                    || state.phase == "failed"
                    || tokio::time::Instant::now() >= deadline
                {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &serde_json::json!({"actor":id,"revision":snapshot.revision,"state":state})
                        )?
                    );
                    if state.phase == "failed" {
                        bail!(
                            "book failed: {}",
                            state.failure.as_deref().unwrap_or("unknown")
                        );
                    }
                    if let Some(path) = output {
                        let reference = state
                            .report
                            .context("book not completed; report unavailable")?;
                        let Artifact::Book(report) = decode(&store.get(&reference).await?)? else {
                            bail!("invalid report artifact");
                        };
                        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Ok(())
}
