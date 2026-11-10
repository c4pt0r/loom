//! Interactive shell over the same client commands as the one-shot CLI.
use crate::client::{Client, ClientCli, Output};
use anyhow::{Context, Result};
use clap::Parser;
use rustyline::{DefaultEditor, error::ReadlineError};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, IsTerminal},
};

const HELP: &str = r#"Commands
  deploy PATH                           Upload a compiled Wasm component -> $module
  put PATH                              Upload input data -> $object
  submit ID MODULE FUNCTION [JSON]      Submit a task -> $task
      --object DIGEST                   Read input from an immutable object
      --dependency TASK_ID              Read input from a preceding task's result
      --attempts N                      Maximum attempts (default 1)
  tasks [--phase PHASE] [--search TEXT]   List tasks (also --limit / --offset)
  status ID                             Inspect a task's status and failure reason
  wait ID [--timeout SECONDS]            Wait for completion -> $result
  result ID [--output PATH]              Read or save a completed task's output
  nodes                                 List live and expired nodes
  actors / messages                     List actors or messages
  create-actor ID MODULE [--state JSON]  Create a durable actor
  send ACTOR --id MESSAGE_ID JSON        Send an actor message
  actor ID                              Read actor state
  get DIGEST / receipt JOB_ID            Read an object or durable receipt
  vars                                  Show session bindings
  help COMMAND                          Show all options for a command
  exit / quit                           Leave the REPL (Ctrl-D also exits)

Example (Docker image includes this component)
  deploy /opt/loom/sort.component.wasm
  submit sort-demo $module sort_chunk '[9, 1, 4, 2]'
  wait $task
  result $task

Quote JSON and paths containing spaces with single quotes. Bindings replace whole
arguments only; JSON contents are never expanded. No shell commands are executed.
Up/Down recall session history. Ctrl-C clears a line or stops wait; submitted tasks
keep running. Namespace and backend are fixed for this session.
"#;

#[derive(Default)]
struct Session {
    bindings: BTreeMap<String, String>,
}
impl Session {
    fn tokens(&self, line: &str) -> Result<Vec<String>> {
        let tokens = shlex::split(line)
            .context("unclosed quote or trailing escape; use single quotes around JSON")?;
        tokens
            .into_iter()
            .map(|token| {
                if let Some(name) = token.strip_prefix('$') {
                    self.bindings
                        .get(name)
                        .cloned()
                        .with_context(|| format!("undefined binding {token}; use vars"))
                } else {
                    Ok(token)
                }
            })
            .collect()
    }

    fn accept(&mut self, output: Output, announce: bool) {
        if !output.text.is_empty() {
            println!("{}", output.text);
        }
        if let Some((key, value)) = output.binding {
            if announce {
                println!("${key} = {value}");
            }
            self.bindings.insert(key.into(), value);
        } else if output.text.is_empty() && announce {
            println!("OK");
        }
    }

    async fn line(&mut self, client: &Client, line: &str, interactive: bool) -> Result<bool> {
        let tokens = self.tokens(line)?;
        let Some(command) = tokens.first() else {
            return Ok(true);
        };
        match command.as_str() {
            "exit" | "quit" => {
                anyhow::ensure!(tokens.len() == 1, "exit and quit take no arguments");
                return Ok(false);
            }
            "vars" => {
                anyhow::ensure!(tokens.len() == 1, "vars takes no arguments");
                if self.bindings.is_empty() {
                    println!("No bindings yet. Use deploy, put, or submit.");
                }
                for (key, value) in &self.bindings {
                    println!("${key} = {value}");
                }
                return Ok(true);
            }
            "help" if tokens.len() == 1 => {
                print!("{HELP}");
                return Ok(true);
            }
            _ => (),
        }
        let parsed =
            match ClientCli::try_parse_from(std::iter::once("loom".to_owned()).chain(tokens)) {
                Ok(parsed) => parsed,
                Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
                    print!("{error}");
                    return Ok(true);
                }
                Err(error) => return Err(error.into()),
            };
        // Cancellation is limited to Wait; do not drop a mutation before its acknowledgement.
        let output = client.execute(parsed.command).await?;
        self.accept(output, interactive);
        Ok(true)
    }
}

pub async fn run(client: Client, namespace: &str) -> Result<()> {
    let mut session = Session::default();
    if io::stdin().is_terminal() {
        let mut editor = DefaultEditor::new()?;
        println!("Loom REPL · namespace {namespace}\nType help for commands. Ctrl-D to exit.");
        let prompt = format!("loom[{namespace}]> ");
        loop {
            match editor.readline(&prompt) {
                Ok(line) => {
                    editor.add_history_entry(line.as_str())?;
                    match session.line(&client, &line, true).await {
                        Ok(false) => break,
                        Ok(true) => (),
                        Err(error) => eprintln!("Error: {error:#}"),
                    }
                }
                Err(ReadlineError::Interrupted) => continue,
                Err(ReadlineError::Eof) => break,
                Err(error) => return Err(error.into()),
            }
        }
    } else {
        // A pipe uses the same grammar, without prompts or terminal escape sequences.
        let mut failed = false;
        let mut lines = io::stdin().lock().lines();
        for line in &mut lines {
            match session.line(&client, &line?, false).await {
                Ok(false) => break,
                Ok(true) => (),
                Err(error) => {
                    failed = true;
                    eprintln!("Error: {error:#}");
                }
            }
        }
        anyhow::ensure!(!failed, "one or more REPL commands failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientCommand;
    use loom_contracts::*;
    use std::sync::Arc;

    fn client() -> Client {
        let store = Arc::new(loom_backend_memory::memory(Arc::new(
            loom_backend_memory::SystemClock,
        )));
        Client {
            backend: store.clone(),
            inspector: store,
        }
    }
    fn command(session: &Session, line: &str) -> ClientCommand {
        ClientCli::try_parse_from(
            std::iter::once("loom".into()).chain(session.tokens(line).unwrap()),
        )
        .unwrap()
        .command
    }

    #[test]
    fn quoting_and_bindings_preserve_json() {
        let mut s = Session::default();
        s.bindings.insert("module".into(), "abc123".into());
        let tokens = s.tokens(r#"submit demo $module f '{"text":"hello world", "literal":"$module", "quote":"it's ok"}'"#);
        assert!(tokens.is_err()); // An apostrophe inside single quotes must be escaped/quoted differently.
        let tokens = s
            .tokens(r#"submit demo $module f '{"text":"hello world", "literal":"$module"}'"#)
            .unwrap();
        assert_eq!(tokens[2], "abc123");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&tokens[4]).unwrap()["literal"],
            "$module"
        );
        assert_eq!(s.tokens("put 'a file.json'").unwrap()[1], "a file.json");
        assert!(s.tokens("get $missing").is_err());
        assert!(s.tokens("submit 'oops").is_err());
        assert!(s.tokens("# comment").unwrap().is_empty());
    }

    #[tokio::test]
    async fn session_submits_queries_and_reads_committed_results() {
        let client = client();
        let mut s = Session::default();
        let module = client.backend.put(b"test module".to_vec()).await.unwrap();
        s.bindings.insert("module".into(), module.digest);
        assert!(
            s.line(&client, "submit first $module echo '[3, 1]'", false)
                .await
                .unwrap()
        );
        assert_eq!(s.bindings["task"], "first");
        // Invalid commands do not end the session or replace successful bindings.
        assert!(
            s.line(&client, "submit bad $module echo 'invalid JSON'", false)
                .await
                .is_err()
        );
        assert_eq!(s.bindings["task"], "first");
        assert!(client.backend.task("bad").await.is_err());
        let status = client
            .execute(command(&s, "status $task"))
            .await
            .unwrap()
            .text;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&status).unwrap()["job"]["phase"],
            "queued"
        );
        let tasks = client
            .execute(command(&s, "tasks --phase queued --search first"))
            .await
            .unwrap()
            .text;
        assert!(tasks.contains("task:first"));
        assert!(client.execute(command(&s, "result $task")).await.is_err());
        assert!(client.wait_for_test("first").await.is_err());
        let worker = NodeSession {
            node: "worker".into(),
            incarnation: "one".into(),
        };
        client.backend.register(worker.clone(), 5000).await.unwrap();
        let claim = client.backend.claim(&worker, 5000).await.unwrap().unwrap();
        let result = client.backend.put(b"[1,3]".to_vec()).await.unwrap();
        client
            .backend
            .commit(
                &claim,
                Completion::Task {
                    output: result.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .execute(command(&s, "result $task"))
                .await
                .unwrap()
                .text,
            "[1,3]"
        );
        assert!(s.line(&client, "wait $task", false).await.unwrap());
        assert_eq!(s.bindings["result"], result.digest);
        s.bindings.insert("object".into(), result.digest);
        s.line(
            &client,
            "submit second $module echo --object $object",
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            client.backend.task("second").await.unwrap().spec.input,
            Input::Object(_)
        ));
        s.line(
            &client,
            "submit third $module echo --dependency second",
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            client.backend.task("third").await.unwrap().spec.input,
            Input::TaskResult(_)
        ));
        assert!(
            client
                .execute(command(&s, "nodes"))
                .await
                .unwrap()
                .text
                .contains("worker")
        );
        assert!(!s.line(&client, "exit", false).await.unwrap());
    }

    #[tokio::test]
    async fn failed_tasks_report_the_actual_error() {
        let client = client();
        let s = Session::default();
        client
            .execute(command(&s, "submit bad module boom null"))
            .await
            .unwrap();
        let worker = NodeSession {
            node: "worker".into(),
            incarnation: "one".into(),
        };
        client.backend.register(worker.clone(), 5000).await.unwrap();
        let claim = client.backend.claim(&worker, 5000).await.unwrap().unwrap();
        client
            .backend
            .fail(&claim, "guest trapped".into())
            .await
            .unwrap();
        for line in ["wait bad", "result bad"] {
            let err = client.execute(command(&s, line)).await.err().unwrap();
            assert!(err.to_string().contains("guest trapped"));
        }
    }

    #[test]
    fn input_modes_and_retry_budget_are_validated() {
        for args in [
            vec!["loom", "submit", "id", "module", "f", "--attempts", "0"],
            vec![
                "loom", "submit", "id", "module", "f", "{}", "--object", "digest",
            ],
            vec![
                "loom",
                "submit",
                "id",
                "module",
                "f",
                "--dependency",
                "other",
                "--object",
                "digest",
            ],
        ] {
            assert!(ClientCli::try_parse_from(args).is_err());
        }
    }
}
