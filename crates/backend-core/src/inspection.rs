use crate::*;

pub fn live(job: &Job, nodes: &BTreeMap<Id, NodeRecord>, now: Millis) -> bool {
    job.receipt.is_none()
        && job.lease_until > now
        && job.owner.as_ref().is_some_and(|o| {
            nodes
                .get(&o.node)
                .is_some_and(|n| n.session == *o && n.lease_until > now)
        })
}
pub fn phase(ledger: &Ledger, job: &Job, now: Millis) -> JobPhase {
    if let Some(receipt) = &job.receipt {
        return if matches!(
            receipt,
            Receipt::Failed(_) | Receipt::Task(TaskOutcome::Failed(_))
        ) {
            JobPhase::Failed
        } else {
            JobPhase::Succeeded
        };
    }
    if live(job, &ledger.nodes, now) {
        return JobPhase::Running;
    }
    if job.attempts > 0 {
        return JobPhase::Retrying;
    }
    if job.available_at > now {
        return JobPhase::Waiting;
    }
    let waiting = match &job.kind {
        Kind::Task(s) => dependencies(s).iter().any(|id| {
            !matches!(
                ledger.jobs.get(id).and_then(|j| j.receipt.as_ref()),
                Some(Receipt::Task(TaskOutcome::Succeeded(_)))
            )
        }),
        Kind::Message(m) => ledger.jobs.values().any(|other| {
            other.seq != job.seq
                && other.receipt.is_none()
                && matches!(&other.kind, Kind::Message(x) if x.actor==m.actor)
                && (live(other, &ledger.nodes, now)
                    || (other.seq < job.seq && (other.available_at <= now || other.attempts > 0)))
        }),
    };
    if waiting {
        JobPhase::Waiting
    } else {
        JobPhase::Queued
    }
}
pub fn dependencies(spec: &TaskSpec) -> Vec<Id> {
    match &spec.input {
        Input::TaskResult(id) => vec![task_job(id)],
        Input::Shuffle { sources, .. } => sources
            .iter()
            .filter_map(|s| {
                if let ShuffleSource::TaskResult(id) = s {
                    Some(task_job(id))
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}
pub fn job_view(id: &str, j: &Job, status: JobPhase, max_attempts: u32) -> JobView {
    let (name, actor) = match &j.kind {
        Kind::Task(s) => (s.function.clone(), s.notify.clone()),
        Kind::Message(m) => ("Actor message".into(), Some(m.actor.clone())),
    };
    let error = match &j.receipt {
        Some(Receipt::Failed(e) | Receipt::Task(TaskOutcome::Failed(e))) => Some(e.clone()),
        _ => j.last_error.clone(),
    };
    let output = match &j.receipt {
        Some(Receipt::Task(TaskOutcome::Succeeded(r))) => Some(r.clone()),
        _ => None,
    };
    let reason = match status {
        JobPhase::Succeeded => "Committed successfully",
        JobPhase::Failed => "Attempt budget exhausted or dependency failed",
        JobPhase::Running => "Live node and work leases",
        JobPhase::Queued => "Ready for a worker",
        JobPhase::Waiting => "Waiting for dependencies, actor turn, or scheduled time",
        JobPhase::Retrying if j.attempts >= max_attempts => {
            "Attempt budget used; awaiting failure finalization"
        }
        JobPhase::Retrying if j.last_error.is_some() || j.failed_epoch.is_some() => {
            "Previous attempt failed; retry pending"
        }
        JobPhase::Retrying => "Owner or work lease expired; awaiting takeover",
    }
    .into();
    JobView {
        id: id.into(),
        seq: j.seq,
        name,
        actor,
        phase: status,
        reason,
        attempts: j.attempts,
        max_attempts,
        owner: j.owner.clone(),
        lease_until: j.lease_until,
        available_at: j.available_at,
        output,
        error,
    }
}
pub fn detail(view: JobView, j: &Job, module: ObjectRef) -> Result<JobDetail> {
    let bytes = match &j.kind {
        Kind::Message(m) => m.payload.clone(),
        Kind::Task(t) => match &t.input {
            Input::Inline(b) => b.clone(),
            other => serde_json::to_vec_pretty(other).map_err(|e| invalid(&e.to_string()))?,
        },
    };
    Ok(JobDetail {
        job: view,
        module,
        input: TextPreview::new(&bytes, 65536),
        receipt: j
            .receipt
            .as_ref()
            .map(|r| serde_json::to_vec_pretty(r).map(|b| TextPreview::new(&b, 65536)))
            .transpose()
            .map_err(|e| invalid(&e.to_string()))?,
    })
}
pub fn node_status(n: &NodeRecord, now: Millis) -> &'static str {
    if n.lease_until <= now {
        "expired"
    } else if n.draining {
        "draining"
    } else {
        "online"
    }
}
pub fn actor_view(a: &ActorSnapshot, pending: u64, running: u64, limit: usize) -> ActorView {
    ActorView {
        id: a.spec.id.clone(),
        handler: a.spec.handler.clone(),
        revision: a.revision,
        pending,
        running,
        state: TextPreview::new(&a.state, limit),
    }
}
impl Ledger {
    fn view(&self, id: &str, j: &Job, now: Millis) -> JobView {
        job_view(id, j, phase(self, j, now), self.max_attempts(j))
    }
    pub fn inspect(&self, q: InspectQuery, now: Millis) -> Result<Inspection> {
        q.validate()?;
        let mut result = Inspection {
            now_ms: now,
            summary: InspectSummary::default(),
            total: 0,
            jobs: vec![],
            nodes: vec![],
            actors: vec![],
        };
        let search = q.search.to_lowercase();
        for (id, j) in &self.jobs {
            let view = self.view(id, j, now);
            let counts = if matches!(j.kind, Kind::Task(_)) {
                &mut result.summary.tasks
            } else {
                &mut result.summary.messages
            };
            *counts.entry(view.phase.as_str().into()).or_default() += 1;
            let selected = matches!(
                (&q.resource, &j.kind),
                (InspectResource::Tasks, Kind::Task(_))
                    | (InspectResource::Messages, Kind::Message(_))
            );
            if selected
                && q.phase.is_none_or(|p| p == view.phase)
                && format!(
                    "{} {} {}",
                    view.id,
                    view.name,
                    view.actor.as_deref().unwrap_or("")
                )
                .to_lowercase()
                .contains(&search)
            {
                result.jobs.push(view);
            }
        }
        for n in self.nodes.values() {
            let status = node_status(n, now);
            *result.summary.nodes.entry(status.into()).or_default() += 1;
            if q.resource == InspectResource::Nodes
                && n.session.node.to_lowercase().contains(&search)
            {
                result.nodes.push(NodeView {
                    session: n.session.clone(),
                    status: status.into(),
                    lease_until: n.lease_until,
                    running: self
                        .jobs
                        .values()
                        .filter(|j| {
                            live(j, &self.nodes, now) && j.owner.as_ref() == Some(&n.session)
                        })
                        .count() as u64,
                });
            }
        }
        result.summary.actors = self.actors.len() as u64;
        if q.resource == InspectResource::Actors {
            for a in self.actors.values().filter(|a| {
                format!("{} {}", a.spec.id, a.spec.handler)
                    .to_lowercase()
                    .contains(&search)
            }) {
                result
                    .actors
                    .push(self.inspect_actor(&a.spec.id, now, 2048)?);
            }
        }
        result.jobs.sort_by_key(|j| std::cmp::Reverse(j.seq));
        result.total = (result.jobs.len() + result.nodes.len() + result.actors.len()) as u64;
        result.jobs = result
            .jobs
            .into_iter()
            .skip(q.offset as usize)
            .take(q.limit as usize)
            .collect();
        result.nodes = result
            .nodes
            .into_iter()
            .skip(q.offset as usize)
            .take(q.limit as usize)
            .collect();
        result.actors = result
            .actors
            .into_iter()
            .skip(q.offset as usize)
            .take(q.limit as usize)
            .collect();
        Ok(result)
    }
    pub fn inspect_job(&self, id: &str, now: Millis) -> Result<JobDetail> {
        let j = self
            .jobs
            .get(id)
            .ok_or_else(|| Error::NotFound(id.into()))?;
        let module = match &j.kind {
            Kind::Task(t) => t.module.clone(),
            Kind::Message(m) => self.actors[&m.actor].spec.module.clone(),
        };
        detail(self.view(id, j, now), j, module)
    }
    pub fn inspect_actor(&self, id: &str, now: Millis, limit: usize) -> Result<ActorView> {
        let a = self
            .actors
            .get(id)
            .ok_or_else(|| Error::NotFound(id.into()))?;
        let pending = self
            .jobs
            .values()
            .filter(|j| j.receipt.is_none() && matches!(&j.kind,Kind::Message(m) if m.actor==id))
            .count() as u64;
        let running = self
            .jobs
            .values()
            .filter(|j| {
                matches!(&j.kind,Kind::Message(m) if m.actor==id) && live(j, &self.nodes, now)
            })
            .count() as u64;
        Ok(actor_view(
            a,
            pending.saturating_sub(running),
            running,
            limit,
        ))
    }
}
#[async_trait]
pub trait InspectionDriver: Send + Sync {
    async fn inspect(&self, q: InspectQuery) -> Result<Inspection>;
    async fn inspect_job(&self, id: &str) -> Result<JobDetail>;
    async fn inspect_actor(&self, id: &str) -> Result<ActorView>;
}
#[async_trait]
impl<D: InspectionDriver> InspectionStore for Store<D> {
    async fn inspect(&self, q: InspectQuery) -> Result<Inspection> {
        self.0.inspect(q).await
    }
    async fn inspect_job(&self, id: &str) -> Result<JobDetail> {
        self.0.inspect_job(id).await
    }
    async fn inspect_actor(&self, id: &str) -> Result<ActorView> {
        self.0.inspect_actor(id).await
    }
}
