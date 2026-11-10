use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use loom_contracts::*;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(name = "loom", disable_version_flag = true)]
pub struct ClientCli {
    #[command(subcommand)]
    pub command: ClientCommand,
}

#[derive(Subcommand)]
pub enum ClientCommand {
    /// Upload a compiled Wasm component and print its digest.
    Deploy {
        component: PathBuf,
    },
    /// Upload immutable input data and print its object digest.
    Put {
        path: PathBuf,
    },
    CreateActor {
        id: String,
        module: String,
        #[arg(long, default_value = "research")]
        handler: String,
        #[arg(long, default_value = "{} ")]
        state: String,
    },
    Send {
        actor: String,
        #[arg(long)]
        id: String,
        payload: String,
    },
    Actor {
        id: String,
    },
    Submit {
        id: String,
        module: String,
        function: String,
        #[arg(default_value = "null", conflicts_with_all = ["dependency", "object"])]
        input: String,
        #[arg(long, conflicts_with = "object")]
        dependency: Option<String>,
        /// Use an existing immutable object as the task input.
        #[arg(long)]
        object: Option<String>,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
        attempts: u32,
    },
    Task {
        id: String,
    },
    Receipt {
        job: String,
    },
    Get {
        digest: String,
    },
    Members,
    /// List tasks, including waiting dependencies and retry reasons.
    Tasks(ListArgs),
    /// List registered nodes, including expired registrations.
    Nodes(ListArgs),
    /// List durable actors and mailbox counts.
    Actors(ListArgs),
    /// List actor messages.
    Messages(ListArgs),
    /// Inspect a task (or a full task:/message: job ID).
    Status {
        id: String,
    },
    /// Read a completed task's output; binary data requires --output.
    Result {
        id: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Wait for task completion; Ctrl-C stops waiting, not the task.
    Wait {
        id: String,
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout: u64,
    },
}

#[derive(Args)]
pub struct ListArgs {
    #[arg(long, default_value = "")]
    search: String,
    #[arg(long, value_parser = ["queued", "running", "waiting", "retrying", "succeeded", "failed"])]
    phase: Option<String>,
    #[arg(long, default_value_t = 25, value_parser = clap::value_parser!(u32).range(1..=100))]
    limit: u32,
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=1000000))]
    offset: u32,
}

pub struct Client {
    pub backend: Arc<dyn Backend>,
    pub inspector: Arc<dyn InspectionStore>,
}

#[derive(Default)]
pub struct Output {
    pub text: String,
    pub binding: Option<(&'static str, String)>,
}
impl Client {
    pub async fn execute(&self, command: ClientCommand) -> Result<Output> {
        let backend = &self.backend;
        let mut output = Output::default();
        match command {
            ClientCommand::Deploy { component } => {
                let digest = backend.put(std::fs::read(component)?).await?.digest;
                output.text = digest.clone();
                output.binding = Some(("module", digest));
            }
            ClientCommand::Put { path } => {
                let digest = backend.put(std::fs::read(path)?).await?.digest;
                output.text = digest.clone();
                output.binding = Some(("object", digest));
            }
            ClientCommand::CreateActor {
                id,
                module,
                handler,
                state,
            } => {
                serde_json::from_str::<serde_json::Value>(&state).context("state must be JSON")?;
                backend
                    .create_actor(ActorSpec {
                        id,
                        module: ObjectRef::new(module),
                        handler,
                        initial_state: state.into_bytes(),
                        supervisor: Supervisor::default(),
                    })
                    .await?;
            }
            ClientCommand::Send { actor, id, payload } => {
                serde_json::from_str::<serde_json::Value>(&payload)
                    .context("payload must be JSON")?;
                backend
                    .send(Envelope {
                        actor,
                        id,
                        payload: payload.into_bytes(),
                    })
                    .await?;
            }
            ClientCommand::Actor { id } => {
                let a = backend.actor(&id).await?;
                let state: serde_json::Value = serde_json::from_slice(&a.state)?;
                output.text =
                    serde_json::json!({"id":a.spec.id,"revision":a.revision,"state":state})
                        .to_string();
            }
            ClientCommand::Submit {
                id,
                module,
                function,
                input,
                dependency,
                object,
                attempts,
            } => {
                output.binding = Some(("task", id.clone()));
                let input = match (dependency, object) {
                    (Some(id), None) => Input::TaskResult(id),
                    (None, Some(digest)) => Input::Object(ObjectRef::new(digest)),
                    (Some(_), Some(_)) => anyhow::bail!("choose either --dependency or --object"),
                    (None, None) => {
                        serde_json::from_str::<serde_json::Value>(&input)?;
                        Input::Inline(input.into_bytes())
                    }
                };
                backend
                    .submit(TaskSpec {
                        id,
                        module: ObjectRef::new(module),
                        function,
                        input,
                        output: loom_contracts::TaskOutput::Value,
                        max_attempts: attempts,
                        notify: None,
                    })
                    .await?;
            }
            ClientCommand::Task { id } => {
                output.text = serde_json::to_string(&backend.task(&id).await?)?
            }
            ClientCommand::Receipt { job } => {
                output.text = serde_json::to_string(&backend.receipt(&job).await?)?
            }
            ClientCommand::Get { digest } => {
                output.text = String::from_utf8(backend.get(&ObjectRef::new(digest)).await?)?
            }
            ClientCommand::Members => {
                output.text = serde_json::to_string(&backend.members().await?)?
            }

            ClientCommand::Tasks(args) => {
                output.text = self.list(InspectResource::Tasks, args).await?
            }
            ClientCommand::Nodes(args) => {
                output.text = self.list(InspectResource::Nodes, args).await?
            }
            ClientCommand::Actors(args) => {
                output.text = self.list(InspectResource::Actors, args).await?
            }
            ClientCommand::Messages(args) => {
                output.text = self.list(InspectResource::Messages, args).await?
            }
            ClientCommand::Status { id } => {
                let job = if id.starts_with("task:") || id.starts_with("message:") {
                    id
                } else {
                    format!("task:{id}")
                };
                output.text =
                    serde_json::to_string_pretty(&self.inspector.inspect_job(&job).await?)?;
            }
            ClientCommand::Result { id, output: path } => {
                let result = completed_output(backend.task(&id).await?)?;
                let bytes = backend.get(&result).await?;
                output.text = if let Some(path) = path {
                    std::fs::write(&path, &bytes)?;
                    format!("Wrote {} bytes to {}", bytes.len(), path.display())
                } else {
                    String::from_utf8(bytes)
                        .context("result is binary; use result ID --output PATH")?
                };
                output.binding = Some(("result", result.digest));
            }
            ClientCommand::Wait { id, timeout } => {
                let result = tokio::select! {
                    result = self.wait(&id, Duration::from_secs(timeout)) => result?,
                    _ = tokio::signal::ctrl_c() => anyhow::bail!("stopped waiting; task {id} continues on workers"),
                };
                output.text = format!("{id}: succeeded\n{}", result.digest);
                output.binding = Some(("result", result.digest));
            }
        }
        Ok(output)
    }
}

impl Client {
    async fn list(&self, resource: InspectResource, args: ListArgs) -> Result<String> {
        let query = InspectQuery {
            resource,
            search: args.search,
            phase: args
                .phase
                .map(|p| serde_json::from_value(serde_json::Value::String(p)))
                .transpose()?,
            offset: args.offset,
            limit: args.limit,
        };
        let page = self.inspector.inspect(query).await?;
        let mut rows = match resource {
            InspectResource::Nodes => vec!["NODE\tSTATUS\tRUNNING\tINCARNATION".to_string()],
            InspectResource::Actors => {
                vec!["ACTOR\tHANDLER\tREVISION\tPENDING\tRUNNING".to_string()]
            }
            _ => vec!["JOB\tFUNCTION\tSTATUS\tATTEMPTS\tWORKER\tDETAIL".to_string()],
        };
        for n in &page.nodes {
            rows.push(format!(
                "{}\t{}\t{}\t{}",
                n.session.node, n.status, n.running, n.session.incarnation
            ));
        }
        for a in &page.actors {
            rows.push(format!(
                "{}\t{}\t{}\t{}\t{}",
                a.id, a.handler, a.revision, a.pending, a.running
            ));
        }
        for j in &page.jobs {
            let detail = match &j.error {
                Some(error) if j.phase == JobPhase::Failed => error.clone(),
                Some(error) => format!("{}; previous attempt: {error}", j.reason),
                None => j.reason.clone(),
            };
            rows.push(format!(
                "{}\t{}\t{}\t{}/{}\t{}\t{}",
                j.id,
                j.name,
                j.phase.as_str(),
                j.attempts,
                j.max_attempts,
                j.owner.as_ref().map(|o| o.node.as_str()).unwrap_or("-"),
                detail
            ));
        }
        let shown = rows.len() - 1;
        rows.push(format!(
            "{shown} shown / {} total (offset {})",
            page.total, args.offset
        ));
        Ok(rows.join("\n"))
    }

    async fn wait(&self, id: &str, timeout: Duration) -> Result<ObjectRef> {
        tokio::time::timeout(timeout, async {
            loop {
                let status = self.backend.task(id).await?;
                if status.outcome.is_some() {
                    return completed_output(status);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for {id}; task continues on workers"))?
    }
}

fn completed_output(status: TaskStatus) -> Result<ObjectRef> {
    match status.outcome {
        Some(TaskOutcome::Succeeded(result)) => Ok(result),
        Some(TaskOutcome::Failed(reason)) => {
            anyhow::bail!("task {} failed: {reason}", status.spec.id)
        }
        None => anyhow::bail!(
            "task {} is not complete; use status or wait",
            status.spec.id
        ),
    }
}

#[cfg(test)]
impl Client {
    pub async fn wait_for_test(&self, id: &str) -> Result<ObjectRef> {
        self.wait(id, Duration::from_millis(10)).await
    }
}
