//! Read-only observability, independent of storage and transport implementations.
use crate::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InspectResource {
    #[default]
    Tasks,
    Messages,
    Actors,
    Nodes,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    Queued,
    Running,
    Waiting,
    Retrying,
    Succeeded,
    Failed,
}
impl JobPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Retrying => "retrying",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InspectQuery {
    pub resource: InspectResource,
    pub search: String,
    pub phase: Option<JobPhase>,
    pub offset: u32,
    pub limit: u32,
}
impl Default for InspectQuery {
    fn default() -> Self {
        Self {
            resource: InspectResource::Tasks,
            search: String::new(),
            phase: None,
            offset: 0,
            limit: 25,
        }
    }
}
impl InspectQuery {
    pub fn validate(&self) -> Result<()> {
        if !(1..=100).contains(&self.limit) || self.offset > 1_000_000 || self.search.len() > 512 {
            return Err(Error::Invalid(
                "limit must be 1..100, offset <=1000000, search <=512 bytes".into(),
            ));
        }
        if self.phase.is_some()
            && matches!(
                self.resource,
                InspectResource::Actors | InspectResource::Nodes
            )
        {
            return Err(Error::Invalid(
                "phase filter applies only to tasks/messages".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InspectSummary {
    pub tasks: BTreeMap<String, u64>,
    pub messages: BTreeMap<String, u64>,
    pub nodes: BTreeMap<String, u64>,
    pub actors: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPreview {
    pub text: String,
    pub bytes: u64,
    pub truncated: bool,
}
impl TextPreview {
    pub fn new(bytes: &[u8], limit: usize) -> Self {
        Self {
            text: String::from_utf8_lossy(&bytes[..bytes.len().min(limit)]).into_owned(),
            bytes: bytes.len() as u64,
            truncated: bytes.len() > limit,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    pub id: Id,
    pub seq: u64,
    pub name: String,
    pub actor: Option<Id>,
    pub phase: JobPhase,
    pub reason: String,
    pub attempts: u32,
    pub max_attempts: u32,
    pub owner: Option<NodeSession>,
    pub lease_until: Millis,
    pub available_at: Millis,
    pub output: Option<ObjectRef>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeView {
    pub session: NodeSession,
    /// online, draining, or expired; expired records are deliberately retained.
    pub status: String,
    pub lease_until: Millis,
    pub running: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorView {
    pub id: Id,
    pub handler: String,
    pub revision: u64,
    /// Uncommitted messages excluding the currently live turn.
    pub pending: u64,
    pub running: u64,
    pub state: TextPreview,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Inspection {
    /// Backend-authoritative snapshot time, not the browser or dashboard host clock.
    pub now_ms: Millis,
    pub summary: InspectSummary,
    pub total: u64,
    pub jobs: Vec<JobView>,
    pub nodes: Vec<NodeView>,
    pub actors: Vec<ActorView>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetail {
    pub job: JobView,
    pub module: ObjectRef,
    pub input: TextPreview,
    pub receipt: Option<TextPreview>,
}
#[async_trait]
pub trait InspectionStore: Send + Sync {
    async fn inspect(&self, query: InspectQuery) -> Result<Inspection>;
    async fn inspect_job(&self, id: &str) -> Result<JobDetail>;
    async fn inspect_actor(&self, id: &str) -> Result<ActorView>;
}
