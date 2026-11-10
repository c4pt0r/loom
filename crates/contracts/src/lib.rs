//! Implementation-independent contracts. No database, transport or Wasm engine dependency.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub type Bytes = Vec<u8>;
pub type Id = String;
pub type Millis = u64;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("lease expired or owner fenced")]
    Fenced,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("backend unavailable (mutation outcome may be unknown): {0}")]
    Unavailable(String),
}

/// A durable immutable blob reference, not an actor address or ownership token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound = "")]
pub struct ObjectRef<T = Bytes> {
    pub digest: String,
    #[serde(skip)]
    marker: PhantomData<fn() -> T>,
}
impl<T> ObjectRef<T> {
    pub fn new(digest: String) -> Self {
        Self {
            digest,
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Input {
    Inline(Bytes),
    Object(ObjectRef),
    TaskResult(Id),
    /// Wait for mapper results, then fold only this partition on the worker.
    Shuffle {
        sources: Vec<ShuffleSource>,
        layout: ShuffleLayout,
        partition: u32,
    },
}

pub const MAX_SHUFFLE_PARTITIONS: u32 = 256;
pub const MAX_SHUFFLE_SOURCES: usize = 1024;

/// Protocol v1: SHA-256(key bytes), first eight bytes as big-endian u64, modulo partitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShuffleLayout {
    pub partitions: u32,
}
impl ShuffleLayout {
    pub fn validate(&self) -> Result<()> {
        if self.partitions == 0 || self.partitions > MAX_SHUFFLE_PARTITIONS {
            return Err(Error::Invalid("shuffle partitions must be 1..256".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShuffleSource {
    TaskResult(Id),
    Manifest(ObjectRef),
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskOutput {
    #[default]
    Value,
    Shuffle(ShuffleLayout),
}

/// Keys are canonical application-defined bytes; values are opaque serialized bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShuffleRecord {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShuffleBlock {
    pub object: ObjectRef,
    pub records: u32,
    pub bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShuffleManifest {
    pub version: u32,
    pub layout: ShuffleLayout,
    /// Index is the partition ID; empty partitions have no blocks.
    pub partitions: Vec<Vec<ShuffleBlock>>,
}

/// Final partitioned result. Entries are reducer outputs in partition-index order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionedDataset {
    pub version: u32,
    pub layout: ShuffleLayout,
    pub partitions: Vec<ObjectRef>,
}

/// Worker-to-guest fold invocation. The state is local to one reducer attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceFrame {
    pub state: Option<Bytes>,
    pub records: Vec<ShuffleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Supervisor {
    /// OneForOne only. Total attempts, including initial execution.
    pub max_attempts: u32,
    pub backoff_ms: Millis,
}
impl Default for Supervisor {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorSpec {
    pub id: Id,
    pub module: ObjectRef,
    pub handler: String,
    pub initial_state: Bytes,
    pub supervisor: Supervisor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSpec {
    pub id: Id,
    pub module: ObjectRef,
    pub function: String,
    pub input: Input,
    #[serde(default)]
    pub output: TaskOutput,
    /// Retries are opt-in; external effects require their own idempotency.
    pub max_attempts: u32,
    pub notify: Option<Id>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskIntent {
    pub function: String,
    pub input: Input,
    #[serde(default)]
    pub output: TaskOutput,
    pub max_attempts: u32,
    pub notify_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendIntent {
    pub actor: Id,
    pub payload: Bytes,
    pub delay_ms: Millis,
}

/// Returned by a guest. The host commits all fields atomically with message receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Decision {
    pub state: Bytes,
    pub reply: Bytes,
    #[serde(default)]
    pub tasks: Vec<TaskIntent>,
    #[serde(default)]
    pub sends: Vec<SendIntent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Envelope {
    /// Globally unique within a backend namespace. Reuse with different content is rejected.
    pub id: Id,
    pub actor: Id,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeSession {
    pub node: Id,
    pub incarnation: Id,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub session: NodeSession,
    pub lease_until: Millis,
    pub draining: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorSnapshot {
    pub spec: ActorSpec,
    pub revision: u64,
    pub state: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskOutcome {
    Succeeded(ObjectRef),
    Failed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompleted {
    pub task_id: Id,
    pub outcome: TaskOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    pub spec: TaskSpec,
    pub attempts: u32,
    pub outcome: Option<TaskOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fence {
    pub job: Id,
    pub owner: NodeSession,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Work {
    Actor {
        snapshot: ActorSnapshot,
        message: Envelope,
    },
    Task {
        spec: TaskSpec,
        input: Input,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claim {
    pub fence: Fence,
    pub attempt: u32,
    pub work: Work,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Completion {
    Actor {
        expected_revision: u64,
        decision: Decision,
    },
    Task {
        output: ObjectRef,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Receipt {
    Actor { revision: u64, reply: Bytes },
    Task(TaskOutcome),
    Failed(String),
}

/// All methods use backend-authoritative time. Expired sessions cannot be renewed.
#[async_trait]
pub trait Membership: Send + Sync {
    async fn register(&self, session: NodeSession, ttl_ms: Millis) -> Result<()>;
    async fn renew(&self, session: &NodeSession, ttl_ms: Millis) -> Result<()>;
    async fn drain(&self, session: &NodeSession) -> Result<()>;
    async fn members(&self) -> Result<Vec<NodeRecord>>;
}

/// Claim/fence validation and commit share one serializable boundary.
/// Retrying the exact same commit returns its original receipt. Never partial success.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    async fn create_actor(&self, spec: ActorSpec) -> Result<()>;
    async fn actor(&self, id: &str) -> Result<ActorSnapshot>;
    async fn send(&self, envelope: Envelope) -> Result<()>;
    async fn submit(&self, spec: TaskSpec) -> Result<()>;
    async fn task(&self, id: &str) -> Result<TaskStatus>;
    async fn claim(&self, owner: &NodeSession, ttl_ms: Millis) -> Result<Option<Claim>>;
    async fn renew_claim(&self, claim: &Claim, ttl_ms: Millis) -> Result<()>;
    async fn commit(&self, claim: &Claim, completion: Completion) -> Result<Receipt>;
    async fn fail(&self, claim: &Claim, reason: String) -> Result<()>;
    async fn receipt(&self, job: &str) -> Result<Option<Receipt>>;
}

/// Namespace-scoped immutable object storage, independently injectable from coordination.
/// `put` returns the SHA-256 digest of the bytes and is idempotent for identical bytes.
/// A successful put must be readable by all workers using this logical object store.
/// Referenced data must outlive retries and dependent tasks; no TTL/GC is implied.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef>;
    async fn get(&self, object: &ObjectRef) -> Result<Bytes>;
}

/// Compatibility name for callers of the original prototype API.
pub use ObjectStore as BlobStore;

/// Membership and task commits must share one fencing/consistency domain.
pub trait ControlStore: Membership + ExecutionStore {}
impl<T: Membership + ExecutionStore + ?Sized> ControlStore for T {}

pub trait Backend: ControlStore + ObjectStore {}
impl<T: ControlStore + ObjectStore + ?Sized> Backend for T {}

mod stores;
pub use stores::Stores;

#[async_trait]
pub trait GuestEngine: Send + Sync {
    async fn task(&self, module: &ObjectRef, function: &str, input: Bytes) -> Result<Bytes>;
    async fn actor(
        &self,
        module: &ObjectRef,
        handler: &str,
        state: Bytes,
        message: Bytes,
    ) -> Result<Decision>;
}

pub fn actor_job(message_id: &str) -> String {
    format!("message:{message_id}")
}
pub fn task_job(task_id: &str) -> String {
    format!("task:{task_id}")
}
