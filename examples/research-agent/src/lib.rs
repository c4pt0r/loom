//! Deterministic research-agent skeleton: fan-out analysis, aggregate, await approval.
//! No LLM credentials or external effects; exercises the actual Wasm boundary.
use loom_guest_sdk::*;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    phase: String,
    #[serde(default)]
    expected: usize,
    #[serde(default)]
    reports: Vec<ObjectRef>,
    #[serde(default)]
    failures: Vec<String>,
}
struct Agent;
struct Analyze;
impl Task for Analyze {
    type Input = String;
    type Output = serde_json::Value;
    fn run(text: String) -> Result<Self::Output, String> {
        Ok(serde_json::json!({"words": text.split_whitespace().count(), "text": text}))
    }
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Message {
    Start { start: Vec<String> },
    Completed(TaskCompleted),
    Approve { approve: bool },
}
impl Guest for Agent {
    fn task(name: String, input: Vec<u8>) -> Result<Vec<u8>, String> {
        match name.as_str() {
            "analyze" => run_task::<Analyze>(&input),
            "identity" => Ok(input),
            "fail" => Err("deliberate guest failure".into()),
            "spin" => loop {
                core::hint::spin_loop();
            },
            _ => Err(format!("unknown task: {name}")),
        }
    }
    fn handle(name: String, state: Vec<u8>, message: Vec<u8>) -> Result<Vec<u8>, String> {
        if name != "research" {
            return Err(format!("unknown actor: {name}"));
        }
        handle_actor::<Research>(&state, &message)
    }
}
struct Research;
impl Actor for Research {
    type State = State;
    type Message = Message;
    type Reply = serde_json::Value;
    fn handle(mut state: State, message: Message) -> Result<Turn<State, Self::Reply>, String> {
        let mut tasks = Vec::new();
        if let Message::Start { start: docs } = message {
            if !state.phase.is_empty() {
                return Err("research already started".into());
            }
            state.expected = docs.len();
            state.phase = if docs.is_empty() {
                "awaiting_approval"
            } else {
                "analyzing"
            }
            .into();
            for doc in docs {
                tasks.push(TaskIntent {
                    function: "analyze".into(),
                    input: Input::Inline(encode(&doc)?),
                    output: TaskOutput::Value,
                    max_attempts: 2,
                    notify_self: true,
                });
            }
        } else if let Message::Completed(done) = message {
            if state.phase != "analyzing" {
                return Err("unexpected task completion".into());
            }
            match done.outcome {
                TaskOutcome::Succeeded(r) => state.reports.push(r),
                TaskOutcome::Failed(e) => state.failures.push(e),
            }
            if state.reports.len() + state.failures.len() == state.expected {
                state.phase = if state.failures.is_empty() {
                    "awaiting_approval"
                } else {
                    "failed"
                }
                .into();
            }
        } else if let Message::Approve { approve: true } = message {
            if state.phase != "awaiting_approval" {
                return Err("not awaiting approval".into());
            }
            state.phase = "done".into();
        } else {
            return Err("unknown message".into());
        }
        let reply = serde_json::json!({"phase":state.phase});
        Ok(Turn {
            state,
            reply,
            tasks,
            sends: vec![],
        })
    }
}
export!(Agent with_types_in loom_guest_sdk::bindings);
