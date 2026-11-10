//! Reference backend state machine, shared by memory and the initial Postgres adapter.
//! Not a runtime dependency. Adapters transact the whole transition or discard it.
use async_trait::async_trait;
use loom_contracts::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Ledger {
    nodes: BTreeMap<Id, NodeRecord>,
    actors: BTreeMap<Id, ActorSnapshot>,
    jobs: BTreeMap<Id, Job>,
    seq: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct Job {
    seq: u64,
    kind: Kind,
    attempts: u32,
    epoch: u64,
    owner: Option<NodeSession>,
    lease_until: Millis,
    available_at: Millis,
    receipt: Option<Receipt>,
    committed: Option<(Fence, Completion)>,
    failed_epoch: Option<u64>,
}
#[derive(Clone, Serialize, Deserialize)]
enum Kind {
    Message(Envelope),
    Task(TaskSpec),
}

pub enum Op {
    Register(NodeSession, Millis),
    Renew(NodeSession, Millis),
    Drain(NodeSession),
    Members,
    Create(ActorSpec),
    Actor(Id),
    Send(Envelope),
    Submit(TaskSpec),
    Task(Id),
    Claim(NodeSession, Millis),
    Extend(Claim, Millis),
    Commit(Claim, Completion),
    Fail(Claim, String),
    Receipt(Id),
}
pub enum Answer {
    Unit,
    Members(Vec<NodeRecord>),
    Actor(ActorSnapshot),
    Task(TaskStatus),
    Claim(Option<Claim>),
    Receipt(Option<Receipt>),
}

fn invalid(s: &str) -> Error {
    Error::Invalid(s.into())
}
fn conflict(s: &str) -> Error {
    Error::Conflict(s.into())
}
fn deadline(now: Millis, ttl: Millis) -> Result<Millis> {
    if ttl == 0 || ttl > 86_400_000 {
        return Err(invalid("TTL must be 1..86400000 ms"));
    }
    now.checked_add(ttl)
        .ok_or_else(|| invalid("timestamp overflow"))
}
fn valid_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 512 {
        return Err(invalid("ID must be 1..512 bytes"));
    }
    Ok(())
}

impl Ledger {
    fn session(&self, session: &NodeSession, now: Millis) -> Result<&NodeRecord> {
        self.nodes
            .get(&session.node)
            .filter(|r| r.session == *session && r.lease_until > now)
            .ok_or(Error::Fenced)
    }
    fn enqueue(&mut self, id: Id, kind: Kind, now: Millis) {
        self.seq += 1;
        self.jobs.insert(
            id,
            Job {
                seq: self.seq,
                kind,
                attempts: 0,
                epoch: 0,
                owner: None,
                lease_until: 0,
                available_at: now,
                receipt: None,
                committed: None,
                failed_epoch: None,
            },
        );
    }
    fn send(&mut self, e: Envelope, now: Millis) -> Result<()> {
        valid_id(&e.id)?;
        if !self.actors.contains_key(&e.actor) {
            return Err(Error::NotFound(e.actor));
        }
        let key = actor_job(&e.id);
        if let Some(j) = self.jobs.get(&key) {
            return if matches!(&j.kind, Kind::Message(old) if *old == e) {
                Ok(())
            } else {
                Err(conflict("message ID reused with different payload"))
            };
        }
        self.enqueue(key, Kind::Message(e), now);
        Ok(())
    }
    fn submit(&mut self, s: TaskSpec, now: Millis) -> Result<()> {
        valid_id(&s.id)?;
        if s.max_attempts == 0 || s.max_attempts > 100 {
            return Err(invalid("attempts must be 1..100"));
        }
        if let Some(target) = &s.notify
            && !self.actors.contains_key(target)
        {
            return Err(Error::NotFound(target.clone()));
        }
        let key = task_job(&s.id);
        if let Some(j) = self.jobs.get(&key) {
            return if matches!(&j.kind, Kind::Task(old) if *old == s) {
                Ok(())
            } else {
                Err(conflict("task ID reused with different input"))
            };
        }
        if let TaskOutput::Shuffle(layout) = &s.output {
            layout.validate()?;
        }
        match &s.input {
            Input::TaskResult(parent) if !self.jobs.contains_key(&task_job(parent)) => {
                return Err(Error::NotFound(parent.clone()));
            }
            Input::Shuffle {
                sources,
                layout,
                partition,
            } => {
                layout.validate()?;
                if *partition >= layout.partitions || sources.len() > MAX_SHUFFLE_SOURCES {
                    return Err(invalid("invalid shuffle partition or too many sources"));
                }
                let mut seen = std::collections::BTreeSet::new();
                for source in sources {
                    if let ShuffleSource::TaskResult(parent) = source {
                        if !seen.insert(parent) {
                            return Err(invalid("duplicate shuffle task dependency"));
                        }
                        let job = self
                            .jobs
                            .get(&task_job(parent))
                            .ok_or_else(|| Error::NotFound(parent.clone()))?;
                        if !matches!(&job.kind, Kind::Task(t) if t.output == TaskOutput::Shuffle(layout.clone()))
                        {
                            return Err(invalid("shuffle source has incompatible output layout"));
                        }
                    }
                }
            }
            _ => (),
        }
        self.enqueue(key, Kind::Task(s), now);
        Ok(())
    }
    fn max_attempts(&self, job: &Job) -> u32 {
        match &job.kind {
            Kind::Task(s) => s.max_attempts,
            Kind::Message(m) => self.actors[&m.actor].spec.supervisor.max_attempts,
        }
    }
    fn notify(&mut self, s: &TaskSpec, outcome: TaskOutcome, now: Millis) -> Result<()> {
        if let Some(actor) = &s.notify {
            let payload = serde_json::to_vec(&TaskCompleted {
                task_id: s.id.clone(),
                outcome,
            })
            .map_err(|e| invalid(&e.to_string()))?;
            self.send(
                Envelope {
                    id: derived_id(&task_job(&s.id), "complete", 0),
                    actor: actor.clone(),
                    payload,
                },
                now,
            )?;
        }
        Ok(())
    }
    fn finish_failure(&mut self, id: &str, reason: String, now: Millis) -> Result<()> {
        let kind = self.jobs[id].kind.clone();
        let receipt = match kind {
            Kind::Task(spec) => {
                let outcome = TaskOutcome::Failed(reason);
                self.notify(&spec, outcome.clone(), now)?;
                Receipt::Task(outcome)
            }
            Kind::Message(_) => Receipt::Failed(reason),
        };
        self.jobs.get_mut(id).unwrap().receipt = Some(receipt);
        Ok(())
    }
    fn check_fence(&self, c: &Claim, now: Millis) -> Result<&Job> {
        self.session(&c.fence.owner, now)?;
        let j = self.jobs.get(&c.fence.job).ok_or(Error::Fenced)?;
        if j.epoch != c.fence.epoch
            || j.owner.as_ref() != Some(&c.fence.owner)
            || j.lease_until <= now
            || j.receipt.is_some()
        {
            return Err(Error::Fenced);
        }
        Ok(j)
    }
    fn claim(&mut self, owner: NodeSession, ttl: Millis, now: Millis) -> Result<Option<Claim>> {
        if self.session(&owner, now)?.draining {
            return Ok(None);
        }
        let until = deadline(now, ttl)?;
        let mut ordered: Vec<_> = self
            .jobs
            .iter()
            .filter(|(_, j)| j.receipt.is_none())
            .map(|(id, j)| (j.seq, id.clone()))
            .collect();
        ordered.sort();
        for (_, id) in ordered {
            let j = self.jobs[&id].clone();
            // A lease remains busy only while BOTH the node and job leases are valid.
            if j.lease_until > now
                && j.owner
                    .as_ref()
                    .is_some_and(|o| self.session(o, now).is_ok())
            {
                continue;
            }
            if j.available_at > now {
                continue;
            }
            if j.attempts >= self.max_attempts(&j) {
                self.finish_failure(&id, "restart/retry budget exhausted".into(), now)?;
                continue;
            }
            let work = match &j.kind {
                Kind::Message(m) => {
                    if self.jobs.iter().any(|(other_id, other)| {
                        *other_id != id
                            && other.receipt.is_none()
                            && matches!(&other.kind, Kind::Message(x) if x.actor == m.actor)
                            && other.lease_until > now
                            && other
                                .owner
                                .as_ref()
                                .is_some_and(|o| self.session(o, now).is_ok())
                    }) {
                        continue;
                    }
                    // One outstanding turn per actor, including backoff; future timers do not block.
                    if self.jobs.values().any(|other| {
                        other.seq < j.seq
                            && other.receipt.is_none()
                            && matches!(&other.kind, Kind::Message(x) if x.actor == m.actor)
                            && (other.available_at <= now || other.attempts > 0)
                    }) {
                        continue;
                    }
                    Work::Actor {
                        snapshot: self.actors[&m.actor].clone(),
                        message: m.clone(),
                    }
                }
                Kind::Task(s) => {
                    let input = match &s.input {
                        Input::TaskResult(parent) => {
                            match self.jobs[&task_job(parent)].receipt.as_ref() {
                                Some(Receipt::Task(TaskOutcome::Succeeded(r))) => {
                                    Input::Object(r.clone())
                                }
                                Some(Receipt::Task(TaskOutcome::Failed(e))) => {
                                    let reason = format!("dependency failed: {e}");
                                    self.finish_failure(&id, reason, now)?;
                                    continue;
                                }
                                _ => continue,
                            }
                        }
                        Input::Shuffle {
                            sources,
                            layout,
                            partition,
                        } => {
                            let mut resolved = Vec::with_capacity(sources.len());
                            let mut waiting = false;
                            let mut failure = None;
                            for source in sources {
                                match source {
                                    ShuffleSource::Manifest(r) => {
                                        resolved.push(ShuffleSource::Manifest(r.clone()))
                                    }
                                    ShuffleSource::TaskResult(parent) => {
                                        match self.jobs[&task_job(parent)].receipt.as_ref() {
                                            Some(Receipt::Task(TaskOutcome::Succeeded(r))) => {
                                                resolved.push(ShuffleSource::Manifest(r.clone()));
                                            }
                                            Some(Receipt::Task(TaskOutcome::Failed(e))) => {
                                                failure = Some(format!(
                                                    "shuffle dependency {parent} failed: {e}"
                                                ));
                                            }
                                            _ => waiting = true,
                                        }
                                    }
                                }
                            }
                            if let Some(reason) = failure {
                                self.finish_failure(&id, reason, now)?;
                                continue;
                            }
                            if waiting {
                                continue;
                            }
                            Input::Shuffle {
                                sources: resolved,
                                layout: layout.clone(),
                                partition: *partition,
                            }
                        }
                        other => other.clone(),
                    };
                    Work::Task {
                        spec: s.clone(),
                        input,
                    }
                }
            };
            let j = self.jobs.get_mut(&id).unwrap();
            j.attempts += 1;
            j.epoch += 1;
            j.owner = Some(owner.clone());
            j.lease_until = until;
            return Ok(Some(Claim {
                fence: Fence {
                    job: id,
                    owner,
                    epoch: j.epoch,
                },
                attempt: j.attempts,
                work,
            }));
        }
        Ok(None)
    }
    pub fn apply(&mut self, op: Op, now: Millis) -> Result<Answer> {
        match op {
            Op::Register(session, ttl) => {
                valid_id(&session.node)?;
                valid_id(&session.incarnation)?;
                let until = deadline(now, ttl)?;
                if let Some(old) = self.nodes.get(&session.node) {
                    if old.session == session {
                        return if old.lease_until > now {
                            Ok(Answer::Unit)
                        } else {
                            Err(Error::Fenced)
                        };
                    }
                    if old.lease_until > now {
                        return Err(conflict("node already registered"));
                    }
                }
                self.nodes.insert(
                    session.node.clone(),
                    NodeRecord {
                        session,
                        lease_until: until,
                        draining: false,
                    },
                );
            }
            Op::Renew(s, ttl) => {
                self.session(&s, now)?;
                self.nodes.get_mut(&s.node).unwrap().lease_until = deadline(now, ttl)?;
            }
            Op::Drain(s) => {
                self.session(&s, now)?;
                self.nodes.get_mut(&s.node).unwrap().draining = true;
            }
            Op::Members => {
                return Ok(Answer::Members(
                    self.nodes
                        .values()
                        .filter(|n| n.lease_until > now)
                        .cloned()
                        .collect(),
                ));
            }
            Op::Create(spec) => {
                valid_id(&spec.id)?;
                if spec.supervisor.max_attempts == 0 || spec.supervisor.max_attempts > 100 {
                    return Err(invalid("restart attempts must be 1..100"));
                }
                if let Some(old) = self.actors.get(&spec.id) {
                    return if old.spec == spec {
                        Ok(Answer::Unit)
                    } else {
                        Err(conflict("actor definition already exists"))
                    };
                }
                self.actors.insert(
                    spec.id.clone(),
                    ActorSnapshot {
                        state: spec.initial_state.clone(),
                        spec,
                        revision: 0,
                    },
                );
            }
            Op::Actor(id) => {
                return self
                    .actors
                    .get(&id)
                    .cloned()
                    .map(Answer::Actor)
                    .ok_or(Error::NotFound(id));
            }
            Op::Send(e) => self.send(e, now)?,
            Op::Submit(s) => self.submit(s, now)?,
            Op::Task(id) => {
                let j = self.jobs.get(&task_job(&id)).ok_or(Error::NotFound(id))?;
                let Kind::Task(spec) = &j.kind else {
                    unreachable!()
                };
                let outcome = match &j.receipt {
                    Some(Receipt::Task(o)) => Some(o.clone()),
                    _ => None,
                };
                return Ok(Answer::Task(TaskStatus {
                    spec: spec.clone(),
                    attempts: j.attempts,
                    outcome,
                }));
            }
            Op::Claim(s, ttl) => return self.claim(s, ttl, now).map(Answer::Claim),
            Op::Extend(c, ttl) => {
                self.check_fence(&c, now)?;
                self.jobs.get_mut(&c.fence.job).unwrap().lease_until = deadline(now, ttl)?;
            }
            Op::Receipt(id) => {
                return Ok(Answer::Receipt(
                    self.jobs.get(&id).and_then(|j| j.receipt.clone()),
                ));
            }
            Op::Commit(c, completion) => {
                if let Some(j) = self.jobs.get(&c.fence.job)
                    && let Some((fence, old)) = &j.committed
                {
                    if *fence == c.fence && *old == completion {
                        return Ok(Answer::Receipt(j.receipt.clone()));
                    }
                    return Err(conflict("job already committed"));
                }
                let j = self.check_fence(&c, now)?.clone();
                let receipt = match (&j.kind, &completion) {
                    (
                        Kind::Message(m),
                        Completion::Actor {
                            expected_revision,
                            decision,
                        },
                    ) => {
                        let actor = &self.actors[&m.actor];
                        if actor.revision != *expected_revision {
                            return Err(conflict("state revision changed"));
                        }
                        let module = actor.spec.module.clone();
                        // Nested IDs are bounded by a digest in runtime/backend helper below.
                        for (i, intent) in decision.tasks.iter().enumerate() {
                            self.submit(
                                TaskSpec {
                                    id: derived_id(&c.fence.job, "spawn", i),
                                    module: module.clone(),
                                    function: intent.function.clone(),
                                    input: intent.input.clone(),
                                    output: intent.output.clone(),
                                    max_attempts: intent.max_attempts,
                                    notify: intent.notify_self.then(|| m.actor.clone()),
                                },
                                now,
                            )?;
                        }
                        for (i, intent) in decision.sends.iter().enumerate() {
                            let at = now
                                .checked_add(intent.delay_ms)
                                .ok_or_else(|| invalid("timer overflow"))?;
                            self.send(
                                Envelope {
                                    id: derived_id(&c.fence.job, "send", i),
                                    actor: intent.actor.clone(),
                                    payload: intent.payload.clone(),
                                },
                                at,
                            )?;
                        }
                        let actor = self.actors.get_mut(&m.actor).unwrap();
                        actor.revision += 1;
                        actor.state = decision.state.clone();
                        Receipt::Actor {
                            revision: actor.revision,
                            reply: decision.reply.clone(),
                        }
                    }
                    (Kind::Task(s), Completion::Task { output }) => {
                        let outcome = TaskOutcome::Succeeded(output.clone());
                        self.notify(s, outcome.clone(), now)?;
                        Receipt::Task(outcome)
                    }
                    _ => return Err(invalid("completion kind does not match job")),
                };
                let j = self.jobs.get_mut(&c.fence.job).unwrap();
                j.receipt = Some(receipt.clone());
                j.committed = Some((c.fence, completion));
                return Ok(Answer::Receipt(Some(receipt)));
            }
            Op::Fail(c, reason) => {
                if self
                    .jobs
                    .get(&c.fence.job)
                    .is_some_and(|j| j.failed_epoch == Some(c.fence.epoch))
                {
                    return Ok(Answer::Unit);
                }
                let j = self.check_fence(&c, now)?.clone();
                let backoff = match &j.kind {
                    Kind::Message(m) => self.actors[&m.actor].spec.supervisor.backoff_ms,
                    _ => 100,
                };
                if j.attempts >= self.max_attempts(&j) {
                    self.finish_failure(&c.fence.job, reason, now)?;
                } else {
                    let job = self.jobs.get_mut(&c.fence.job).unwrap();
                    job.available_at =
                        now.saturating_add(backoff.saturating_mul(u64::from(j.attempts)));
                    job.lease_until = 0;
                    job.owner = None;
                }
                self.jobs.get_mut(&c.fence.job).unwrap().failed_epoch = Some(c.fence.epoch);
            }
        }
        Ok(Answer::Unit)
    }
}

pub fn derived_id(parent: &str, kind: &str, index: usize) -> String {
    use std::fmt::Write;
    // Stable collision-resistant IDs, independent of process or backend.
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(parent, kind, index)).unwrap();
    let digest = Sha256::digest(bytes);
    let mut out = format!("{kind}:");
    for b in digest {
        write!(out, "{b:02x}").unwrap();
    }
    out
}

#[async_trait]
pub trait Driver: Send + Sync {
    async fn transact(&self, op: Op) -> Result<Answer>;
    async fn put_blob(&self, bytes: Bytes) -> Result<ObjectRef>;
    async fn get_blob(&self, object: &ObjectRef) -> Result<Bytes>;
}
pub struct Store<D>(pub D);

#[async_trait]
impl<D: Driver> Membership for Store<D> {
    async fn register(&self, s: NodeSession, ttl: Millis) -> Result<()> {
        self.0.transact(Op::Register(s, ttl)).await?;
        Ok(())
    }
    async fn renew(&self, s: &NodeSession, ttl: Millis) -> Result<()> {
        self.0.transact(Op::Renew(s.clone(), ttl)).await?;
        Ok(())
    }
    async fn drain(&self, s: &NodeSession) -> Result<()> {
        self.0.transact(Op::Drain(s.clone())).await?;
        Ok(())
    }
    async fn members(&self) -> Result<Vec<NodeRecord>> {
        match self.0.transact(Op::Members).await? {
            Answer::Members(x) => Ok(x),
            _ => unreachable!(),
        }
    }
}
#[async_trait]
impl<D: Driver> ExecutionStore for Store<D> {
    async fn create_actor(&self, s: ActorSpec) -> Result<()> {
        self.0.transact(Op::Create(s)).await?;
        Ok(())
    }
    async fn actor(&self, id: &str) -> Result<ActorSnapshot> {
        match self.0.transact(Op::Actor(id.into())).await? {
            Answer::Actor(x) => Ok(x),
            _ => unreachable!(),
        }
    }
    async fn send(&self, e: Envelope) -> Result<()> {
        self.0.transact(Op::Send(e)).await?;
        Ok(())
    }
    async fn submit(&self, s: TaskSpec) -> Result<()> {
        self.0.transact(Op::Submit(s)).await?;
        Ok(())
    }
    async fn task(&self, id: &str) -> Result<TaskStatus> {
        match self.0.transact(Op::Task(id.into())).await? {
            Answer::Task(x) => Ok(x),
            _ => unreachable!(),
        }
    }
    async fn claim(&self, s: &NodeSession, ttl: Millis) -> Result<Option<Claim>> {
        match self.0.transact(Op::Claim(s.clone(), ttl)).await? {
            Answer::Claim(x) => Ok(x),
            _ => unreachable!(),
        }
    }
    async fn renew_claim(&self, c: &Claim, ttl: Millis) -> Result<()> {
        self.0.transact(Op::Extend(c.clone(), ttl)).await?;
        Ok(())
    }
    async fn commit(&self, c: &Claim, x: Completion) -> Result<Receipt> {
        match self.0.transact(Op::Commit(c.clone(), x)).await? {
            Answer::Receipt(Some(x)) => Ok(x),
            _ => unreachable!(),
        }
    }
    async fn fail(&self, c: &Claim, why: String) -> Result<()> {
        self.0.transact(Op::Fail(c.clone(), why)).await?;
        Ok(())
    }
    async fn receipt(&self, id: &str) -> Result<Option<Receipt>> {
        match self.0.transact(Op::Receipt(id.into())).await? {
            Answer::Receipt(x) => Ok(x),
            _ => unreachable!(),
        }
    }
}
#[async_trait]
impl<D: Driver> ObjectStore for Store<D> {
    async fn put(&self, b: Bytes) -> Result<ObjectRef> {
        self.0.put_blob(b).await
    }
    async fn get(&self, r: &ObjectRef) -> Result<Bytes> {
        self.0.get_blob(r).await
    }
}
