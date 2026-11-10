mod client;
mod repl;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use loom_contracts::*;
use loom_runtime::Node;
use std::{path::PathBuf, sync::Arc};

#[derive(Parser)]
#[command(name = "loom", about = "Rust + Wasm distributed execution prototype")]
struct Cli {
    #[arg(long, env = "LOOM_DATABASE_URL", global = true)]
    database_url: Option<String>,
    #[arg(long, default_value = "default", global = true)]
    namespace: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Compile the bundled Rust guest and encode its Wasm component.
    Build,
    /// Encode a Rust guest core module containing WIT metadata into a component.
    Component { input: PathBuf, output: PathBuf },
    /// Run the complete research/approval demo in a volatile local backend.
    Demo { component: PathBuf },
    /// Register and poll a shared durable backend with bounded execution slots.
    Node {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = 5000)]
        lease_ms: u64,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=256))]
        slots: u16,
    },
    /// Serve a read-only dashboard for this namespace.
    Dashboard {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: std::net::SocketAddr,
    },
    /// Open an interactive client (type help for commands).
    Repl,
    #[command(flatten)]
    Client(client::ClientCommand),
}
fn session(node: &str) -> NodeSession {
    NodeSession {
        node: node.into(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    }
}
fn encode_component(input: PathBuf, output: PathBuf) -> Result<()> {
    let module = std::fs::read(input)?;
    let bytes = wit_component::ComponentEncoder::default()
        .module(&module)?
        .validate(true)
        .encode()?;
    std::fs::write(&output, bytes)?;
    println!("{}", output.display());
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Build => {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let status = std::process::Command::new("cargo")
                .current_dir(&root)
                .args([
                    "build",
                    "--locked",
                    "-p",
                    "loom-example-agent",
                    "--target",
                    "wasm32-unknown-unknown",
                    "--release",
                ])
                .status()?;
            anyhow::ensure!(
                status.success(),
                "guest build failed; install target: rustup target add wasm32-unknown-unknown"
            );
            return encode_component(
                root.join("target/wasm32-unknown-unknown/release/loom_example_agent.wasm"),
                root.join("target/research-agent.component.wasm"),
            );
        }
        Command::Component { input, output } => return encode_component(input, output),
        Command::Demo { component } => return demo(component).await,
        _ => (),
    }
    let url = cli
        .database_url
        .context("set LOOM_DATABASE_URL or --database-url (demo uses memory)")?;
    let postgres = Arc::new(loom_backend_postgres::connect(&url, &cli.namespace).await?);
    if let Command::Dashboard { bind } = cli.command {
        loom_dashboard::serve(
            loom_dashboard::Dashboard {
                inspector: postgres.clone(),
                objects: postgres,
                namespace: cli.namespace,
            },
            bind,
        )
        .await?;
        return Ok(());
    }
    let inspector: Arc<dyn InspectionStore> = postgres.clone();
    // Default adapters share Postgres; an independent ObjectStore can be supplied here.
    let backend: Arc<dyn Backend> = Arc::new(Stores::new(postgres.clone(), postgres));
    match cli.command {
        Command::Node {
            id,
            lease_ms,
            slots,
        } => {
            anyhow::ensure!(lease_ms >= 1000, "node lease must be at least 1000 ms");
            let engine = Arc::new(loom_wasm_host::WasmEngine::new(backend.clone())?);
            let node = Node {
                store: backend,
                engine,
                session: session(&id),
                lease_ms,
            };
            eprintln!("NODE {} {}", node.session.node, node.session.incarnation);
            eprintln!("SLOTS {slots}");
            node.run_with_slots(usize::from(slots), async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
        }
        Command::Client(command) => {
            let result = client::Client { backend, inspector }
                .execute(command)
                .await?;
            if !result.text.is_empty() {
                println!("{}", result.text);
            }
        }
        Command::Repl => repl::run(client::Client { backend, inspector }, &cli.namespace).await?,
        _ => unreachable!(),
    }
    Ok(())
}
async fn demo(component: PathBuf) -> Result<()> {
    let backend: Arc<dyn Backend> = Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::SystemClock,
    )));
    let module = backend.put(std::fs::read(component)?).await?;
    backend
        .create_actor(ActorSpec {
            id: "research".into(),
            module,
            handler: "research".into(),
            initial_state: b"{}".to_vec(),
            supervisor: Supervisor::default(),
        })
        .await?;
    backend
        .send(Envelope {
            id: "start".into(),
            actor: "research".into(),
            payload: br#"{"start":["Rust Wasm actors","Durable agent execution"]}"#.to_vec(),
        })
        .await?;
    let node = Node {
        store: backend.clone(),
        engine: Arc::new(loom_wasm_host::WasmEngine::new(backend.clone())?),
        session: session("local"),
        lease_ms: 60_000,
    };
    node.register().await?;
    while node.tick().await? {}
    let before = backend.actor("research").await?;
    let state: serde_json::Value = serde_json::from_slice(&before.state)?;
    anyhow::ensure!(
        state["phase"] == "awaiting_approval",
        "unexpected state: {state}"
    );
    println!("before approval: {state}");
    backend
        .send(Envelope {
            id: "approval".into(),
            actor: "research".into(),
            payload: br#"{"approve":true}"#.to_vec(),
        })
        .await?;
    while node.tick().await? {}
    let after = backend.actor("research").await?;
    let state: serde_json::Value = serde_json::from_slice(&after.state)?;
    anyhow::ensure!(
        state["phase"] == "done" && after.revision == 4,
        "incorrect final state"
    );
    println!("after approval: {state}");
    println!("PASS: Rust guest → Wasm → Actor → Tasks → ObjectRefs → approval");
    Ok(())
}
