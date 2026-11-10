//! Rust guest API. This ABI deliberately has no ambient filesystem/network/DB access.
pub use loom_contracts::{
    Decision, Input, ObjectRef, ReduceFrame, SendIntent, ShuffleRecord, TaskCompleted, TaskIntent,
    TaskOutcome, TaskOutput,
};
pub use serde_json;
pub mod bindings {
    wit_bindgen::generate!({ path: "../../wit", world: "guest", pub_export_macro: true });
}
pub use bindings::{Guest, export};
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|e| e.to_string())
}

/// Typed Rust task; the component export handles serialization at the boundary.
pub trait Task {
    type Input: serde::de::DeserializeOwned;
    type Output: serde::Serialize;
    fn run(input: Self::Input) -> Result<Self::Output, String>;
}
pub fn run_task<T: Task>(input: &[u8]) -> Result<Vec<u8>, String> {
    encode(&T::run(decode(input)?)?)
}

/// A short, deterministic state transition. No ambient I/O inside a durable turn.
pub trait Actor {
    type State: serde::de::DeserializeOwned + serde::Serialize;
    type Message: serde::de::DeserializeOwned;
    type Reply: serde::Serialize;
    fn handle(
        state: Self::State,
        message: Self::Message,
    ) -> Result<Turn<Self::State, Self::Reply>, String>;
}
pub struct Turn<S, R> {
    pub state: S,
    pub reply: R,
    pub tasks: Vec<TaskIntent>,
    pub sends: Vec<SendIntent>,
}
pub fn handle_actor<A: Actor>(state: &[u8], message: &[u8]) -> Result<Vec<u8>, String> {
    let turn = A::handle(decode(state)?, decode(message)?)?;
    encode(&Decision {
        state: encode(&turn.state)?,
        reply: encode(&turn.reply)?,
        tasks: turn.tasks,
        sends: turn.sends,
    })
}

/// Emit an opaque value under an application-defined canonical key.
pub fn record<T: serde::Serialize>(key: &[u8], value: &T) -> Result<ShuffleRecord, String> {
    Ok(ShuffleRecord {
        key: key.to_vec(),
        value: encode(value)?,
    })
}

/// A bounded fold invoked once per shuffle block. No effects or persistent local state.
pub trait Reducer {
    type State: Default + serde::Serialize + serde::de::DeserializeOwned;
    fn fold(state: Self::State, records: Vec<ShuffleRecord>) -> Result<Self::State, String>;
}
pub fn run_reducer<R: Reducer>(input: &[u8]) -> Result<Vec<u8>, String> {
    let frame: ReduceFrame = decode(input)?;
    let state = match frame.state {
        Some(bytes) => decode(&bytes)?,
        None => R::State::default(),
    };
    encode(&R::fold(state, frame.records)?)
}
