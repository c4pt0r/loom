//! Row-oriented PostgreSQL coordination with immutable object storage.
use async_trait::async_trait;
use loom_backend_core::{Answer, Driver, Job, Kind, Ledger, Op, Store, derived_id};
use loom_contracts::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Transaction};

mod inspection;
mod schema;
pub struct PostgresDriver {
    clients: Vec<Mutex<Client>>,
    next: AtomicUsize,
    namespace: String,
}
pub type PostgresStore = Store<PostgresDriver>;
fn db(e: tokio_postgres::Error) -> Error {
    if e.code()
        .is_some_and(|code| matches!(code.code(), "40P01" | "40001"))
    {
        Error::Unavailable(format!("retryable transaction: {e}"))
    } else {
        Error::Unavailable(e.to_string())
    }
}
fn encode<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| Error::Invalid(e.to_string()))
}
fn sql_integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::Invalid("value exceeds PostgreSQL bigint limit".into()))
}
fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))
}
async fn client(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(db)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection closed: {e}");
        }
    });
    Ok(client)
}
pub async fn connect(url: &str, namespace: &str) -> Result<PostgresStore> {
    if namespace.is_empty() {
        return Err(Error::Invalid("namespace required".into()));
    }
    let mut first = client(url).await?;
    schema::initialize(&mut first, namespace).await?;
    let (a, b, c) = tokio::try_join!(client(url), client(url), client(url))?;
    Ok(Store(PostgresDriver {
        clients: vec![first, a, b, c].into_iter().map(Mutex::new).collect(),
        next: AtomicUsize::new(0),
        namespace: namespace.into(),
    }))
}
async fn now(tx: &Transaction<'_>) -> Result<u64> {
    let time: i64 = tx
        .query_one(
            "SELECT floor(extract(epoch from clock_timestamp())*1000)::bigint",
            &[],
        )
        .await
        .map_err(db)?
        .get(0);
    Ok(time as u64)
}
fn dependencies(spec: &TaskSpec) -> Vec<String> {
    match &spec.input {
        Input::TaskResult(id) => vec![task_job(id)],
        Input::Shuffle { sources, .. } => sources
            .iter()
            .filter_map(|s| match s {
                ShuffleSource::TaskResult(id) => Some(task_job(id)),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}
impl PostgresDriver {
    async fn lock(&self, tx: &Transaction<'_>, kind: &str, id: &str, skip: bool) -> Result<bool> {
        let key = encode(&(&self.namespace, kind, id))?;
        if skip {
            Ok(tx
                .query_one(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    &[&key],
                )
                .await
                .map_err(db)?
                .get(0))
        } else {
            tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                &[&key],
            )
            .await
            .map_err(db)?;
            Ok(true)
        }
    }
    async fn job(&self, tx: &Transaction<'_>, id: &str, write: bool) -> Result<Option<Job>> {
        let sql = if write {
            "SELECT body FROM loom_jobs WHERE namespace=$1 AND id=$2 FOR UPDATE"
        } else {
            "SELECT body FROM loom_jobs WHERE namespace=$1 AND id=$2"
        };
        tx.query_opt(sql, &[&self.namespace, &id])
            .await
            .map_err(db)?
            .map(|r| decode(r.get(0)))
            .transpose()
    }
    async fn load_node(
        &self,
        tx: &Transaction<'_>,
        ledger: &mut Ledger,
        id: &str,
        write: bool,
    ) -> Result<()> {
        let sql = if write {
            "SELECT body FROM loom_nodes WHERE namespace=$1 AND id=$2 FOR UPDATE"
        } else {
            "SELECT body FROM loom_nodes WHERE namespace=$1 AND id=$2 FOR SHARE"
        };
        if let Some(row) = tx
            .query_opt(sql, &[&self.namespace, &id])
            .await
            .map_err(db)?
        {
            ledger.nodes.insert(id.into(), decode(row.get(0))?);
        }
        Ok(())
    }
    async fn load_actor(&self, tx: &Transaction<'_>, ledger: &mut Ledger, id: &str) -> Result<()> {
        if let Some(row) = tx
            .query_opt(
                "SELECT body FROM loom_actors WHERE namespace=$1 AND id=$2",
                &[&self.namespace, &id],
            )
            .await
            .map_err(db)?
        {
            ledger.actors.insert(id.into(), decode(row.get(0))?);
        }
        Ok(())
    }
    async fn load_dependencies(
        &self,
        tx: &Transaction<'_>,
        ledger: &mut Ledger,
        spec: &TaskSpec,
    ) -> Result<()> {
        let ids = dependencies(spec);
        if !ids.is_empty() {
            for row in tx
                .query(
                    "SELECT id,body FROM loom_jobs WHERE namespace=$1 AND id=ANY($2)",
                    &[&self.namespace, &ids],
                )
                .await
                .map_err(db)?
            {
                let id: String = row.get(0);
                if let std::collections::btree_map::Entry::Vacant(entry) = ledger.jobs.entry(id) {
                    entry.insert(decode(row.get(1))?);
                }
            }
        }
        Ok(())
    }
    async fn save(&self, tx: &Transaction<'_>, before: &Ledger, after: &mut Ledger) -> Result<()> {
        for (id, node) in &after.nodes {
            if before.nodes.get(id).map(encode).transpose()? == Some(encode(node)?) {
                continue;
            }
            tx.execute("INSERT INTO loom_nodes(namespace,id,incarnation,lease_until,body) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,id) DO UPDATE SET incarnation=$3,lease_until=$4,body=$5",
                &[&self.namespace,id,&node.session.incarnation,&sql_integer(node.lease_until)?,&encode(node)?]).await.map_err(db)?;
        }
        for (id, actor) in &after.actors {
            if before.actors.get(id).map(encode).transpose()? == Some(encode(actor)?) {
                continue;
            }
            tx.execute("INSERT INTO loom_actors(namespace,id,body) VALUES($1,$2,$3) ON CONFLICT(namespace,id) DO UPDATE SET body=$3", &[&self.namespace,id,&encode(actor)?]).await.map_err(db)?;
        }
        // Allocate ordering only for newly created jobs, preserving one transaction's outbox order.
        let mut added: Vec<_> = after
            .jobs
            .iter()
            .filter(|(id, _)| !before.jobs.contains_key(*id))
            .map(|(id, j)| (j.seq, id.clone()))
            .collect();
        added.sort();
        for (_, id) in added {
            let seq: i64 = tx
                .query_one("SELECT nextval('loom_job_sequence')", &[])
                .await
                .map_err(db)?
                .get(0);
            after.jobs.get_mut(&id).unwrap().seq = seq as u64;
        }
        for (id, job) in &after.jobs {
            if before.jobs.get(id).map(encode).transpose()? == Some(encode(job)?) {
                continue;
            }
            let actor = match &job.kind {
                Kind::Message(e) => Some(e.actor.as_str()),
                _ => None,
            };
            let owner = job.owner.as_ref().map(|o| o.node.as_str());
            let incarnation = job.owner.as_ref().map(|o| o.incarnation.as_str());
            let failed = matches!(
                job.receipt,
                Some(Receipt::Task(TaskOutcome::Failed(_))) | Some(Receipt::Failed(_))
            );
            tx.execute("INSERT INTO loom_jobs(namespace,id,seq,actor,owner,incarnation,lease_until,available_at,attempts,done,failed,body) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(namespace,id) DO UPDATE SET owner=$5,incarnation=$6,lease_until=$7,available_at=$8,attempts=$9,done=$10,failed=$11,body=$12",
                &[&self.namespace,id,&sql_integer(job.seq)?,&actor,&owner,&incarnation,&sql_integer(job.lease_until)?,&sql_integer(job.available_at)?,&(job.attempts as i32),&job.receipt.is_some(),&failed,&encode(job)?]).await.map_err(db)?;
            if !before.jobs.contains_key(id)
                && let Kind::Task(spec) = &job.kind
            {
                for parent in dependencies(spec) {
                    tx.execute(
                        "INSERT INTO loom_dependencies(namespace,job,parent) VALUES($1,$2,$3)",
                        &[&self.namespace, id, &parent],
                    )
                    .await
                    .map_err(db)?;
                }
            }
        }
        Ok(())
    }
    async fn prepare(&self, tx: &Transaction<'_>, op: &Op) -> Result<Ledger> {
        let mut ledger = Ledger::default();
        let mut writes = BTreeSet::<String>::new();
        let mut actors = BTreeSet::<String>::new();
        let mut specs = Vec::<TaskSpec>::new();
        let mut node = None;
        let mut write_node = false;
        match op {
            Op::Register(s,_) | Op::Renew(s,_) | Op::Drain(s) => { node = Some(s.node.clone()); write_node = true; }
            Op::Create(s) => { actors.insert(s.id.clone()); }
            Op::Send(e) => { writes.insert(actor_job(&e.id)); actors.insert(e.actor.clone()); }
            Op::Submit(s) => { writes.insert(task_job(&s.id)); specs.push(s.clone()); }
            Op::Extend(c,_) | Op::Commit(c,_) | Op::Fail(c,_) => {
                node = Some(c.fence.owner.node.clone());
                writes.insert(c.fence.job.clone());
                if let Some(job) = self.job(tx, &c.fence.job, false).await? {
                    match job.kind {
                        Kind::Message(e) => {
                            actors.insert(e.actor.clone());
                            if let Op::Commit(_, Completion::Actor { decision, .. }) = op {
                                self.load_actor(tx, &mut ledger, &e.actor).await?;
                                if let Some(actor) = ledger.actors.get(&e.actor) {
                                    for (i,intent) in decision.tasks.iter().enumerate() {
                                        let id = derived_id(&c.fence.job,"spawn",i);
                                        writes.insert(task_job(&id));
                                        specs.push(TaskSpec { id, module: actor.spec.module.clone(), function:intent.function.clone(), input:intent.input.clone(), output:intent.output.clone(), max_attempts:intent.max_attempts, notify:intent.notify_self.then(|| e.actor.clone()) });
                                    }
                                }
                                for (i,intent) in decision.sends.iter().enumerate() {
                                    actors.insert(intent.actor.clone());
                                    writes.insert(actor_job(&derived_id(&c.fence.job,"send",i)));
                                }
                            }
                        }
                        Kind::Task(s) => {
                            if let Some(actor) = s.notify {
                                actors.insert(actor);
                                writes.insert(actor_job(&derived_id(&c.fence.job,"complete",0)));
                            }
                        }
                    }
                }
            }
            Op::Actor(id) => { self.load_actor(tx, &mut ledger, id).await?; }
            Op::Task(id) => { let key = task_job(id); if let Some(j) = self.job(tx,&key,false).await? { ledger.jobs.insert(key,j); } }
            Op::Receipt(id) => { if let Some(j) = self.job(tx,id,false).await? { ledger.jobs.insert(id.clone(),j); } }
            Op::Members => {
                for row in tx.query("SELECT id,body FROM loom_nodes WHERE namespace=$1 AND lease_until > floor(extract(epoch from clock_timestamp())*1000)::bigint", &[&self.namespace]).await.map_err(db)? { ledger.nodes.insert(row.get(0),decode(row.get(1))?); }
            }
            Op::Claim(_,_) => unreachable!(),
        }
        for spec in &specs {
            if let Some(actor) = &spec.notify {
                actors.insert(actor.clone());
            }
        }
        // Stable lock ordering, including not-yet-created deduplication keys.
        for id in &writes {
            self.lock(tx, "job", id, false).await?;
        }
        for id in &actors {
            self.lock(tx, "actor", id, false).await?;
        }
        if let Some(id) = node {
            if write_node {
                self.lock(tx, "node", &id, false).await?;
            }
            self.load_node(tx, &mut ledger, &id, write_node).await?;
        }
        for id in writes {
            if let Some(j) = self.job(tx, &id, true).await? {
                ledger.jobs.insert(id, j);
            }
        }
        for id in actors {
            self.load_actor(tx, &mut ledger, &id).await?;
        }
        for spec in specs {
            self.load_dependencies(tx, &mut ledger, &spec).await?;
        }
        ledger.seq = ledger.jobs.values().map(|j| j.seq).max().unwrap_or(0);
        Ok(ledger)
    }
    async fn claim(
        &self,
        tx: &Transaction<'_>,
        owner: &NodeSession,
        ttl: Millis,
    ) -> Result<Answer> {
        let mut skipped: Vec<String> = Vec::new();
        // Bound one poll's work; SQL eliminates dependency and actor-order barriers.
        for _ in 0..64 {
            let time = now(tx).await? as i64;
            let candidate = tx.query_opt("SELECT j.id,j.body FROM loom_jobs j LEFT JOIN loom_nodes n ON n.namespace=j.namespace AND n.id=j.owner
                WHERE j.namespace=$1 AND NOT j.done AND j.available_at <= $2 AND NOT(j.id=ANY($3))
                AND (j.lease_until <= $2 OR n.id IS NULL OR n.incarnation<>j.incarnation OR n.lease_until <= $2)
                AND (NOT EXISTS(SELECT 1 FROM loom_dependencies d JOIN loom_jobs p ON p.namespace=d.namespace AND p.id=d.parent WHERE d.namespace=j.namespace AND d.job=j.id AND NOT p.done)
                     OR EXISTS(SELECT 1 FROM loom_dependencies d JOIN loom_jobs p ON p.namespace=d.namespace AND p.id=d.parent WHERE d.namespace=j.namespace AND d.job=j.id AND p.failed))
                AND (j.actor IS NULL OR NOT EXISTS(SELECT 1 FROM loom_jobs earlier WHERE earlier.namespace=j.namespace AND earlier.actor=j.actor AND NOT earlier.done AND earlier.seq<j.seq AND (earlier.available_at <= $2 OR earlier.attempts>0)))
                ORDER BY j.seq LIMIT 1 FOR UPDATE OF j SKIP LOCKED", &[&self.namespace,&time,&skipped]).await.map_err(db)?;
            let Some(row) = candidate else {
                let mut ledger = Ledger::default();
                self.load_node(tx, &mut ledger, &owner.node, false).await?;
                return ledger.apply(Op::Claim(owner.clone(), ttl), now(tx).await?);
            };
            let id: String = row.get(0);
            let job: Job = decode(row.get(1))?;
            skipped.push(id.clone());
            if !self.lock(tx, "job", &id, true).await? {
                continue;
            }
            let target = match &job.kind {
                Kind::Message(e) => Some(&e.actor),
                Kind::Task(s) => s.notify.as_ref(),
            };
            if let Some(actor) = target
                && !self.lock(tx, "actor", actor, true).await?
            {
                continue;
            }
            let notification = if matches!(job.kind, Kind::Task(_)) && target.is_some() {
                Some(actor_job(&derived_id(&id, "complete", 0)))
            } else {
                None
            };
            if let Some(key) = &notification
                && !self.lock(tx, "job", key, true).await?
            {
                continue;
            }
            let mut ledger = Ledger::default();
            ledger.jobs.insert(id.clone(), job.clone());
            if let Some(actor) = target {
                self.load_actor(tx, &mut ledger, actor).await?;
            }
            if let Kind::Message(e) = &job.kind {
                for row in tx.query("SELECT id,body FROM loom_jobs WHERE namespace=$1 AND actor=$2 AND NOT done", &[&self.namespace,&e.actor]).await.map_err(db)? { ledger.jobs.insert(row.get(0),decode(row.get(1))?); }
            }
            if let Kind::Task(s) = &job.kind {
                self.load_dependencies(tx, &mut ledger, s).await?;
            }
            if let Some(key) = notification
                && let Some(j) = self.job(tx, &key, true).await?
            {
                ledger.jobs.insert(key, j);
            }
            let mut nodes = BTreeSet::from([owner.node.clone()]);
            for job in ledger.jobs.values() {
                if let Some(s) = &job.owner {
                    nodes.insert(s.node.clone());
                }
            }
            for node in nodes {
                self.load_node(tx, &mut ledger, &node, false).await?;
            }
            ledger.seq = ledger.jobs.values().map(|j| j.seq).max().unwrap_or(0);
            let before = ledger.clone();
            let answer = ledger.claim_one(&id, owner.clone(), ttl, now(tx).await?)?;
            self.save(tx, &before, &mut ledger).await?;
            if matches!(answer, Answer::Claim(Some(_))) {
                return Ok(answer);
            }
        }
        Ok(Answer::Claim(None))
    }
    async fn once(&self, op: Op) -> Result<Answer> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let mut client = self.clients[index].lock().await;
        let tx = client.transaction().await.map_err(db)?;
        let answer = if let Op::Claim(owner, ttl) = op {
            self.claim(&tx, &owner, ttl).await?
        } else {
            let mut ledger = self.prepare(&tx, &op).await?;
            let before = ledger.clone();
            let answer = ledger.apply(op, now(&tx).await?)?;
            self.save(&tx, &before, &mut ledger).await?;
            answer
        };
        tx.commit().await.map_err(db)?;
        Ok(answer)
    }
}
#[async_trait]
impl Driver for PostgresDriver {
    async fn transact(&self, op: Op) -> Result<Answer> {
        for attempt in 0..4 {
            match self.once(op.clone()).await {
                Err(Error::Unavailable(reason))
                    if reason.starts_with("retryable transaction:") && attempt < 3 =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt + 1))).await;
                }
                result => return result,
            }
        }
        unreachable!()
    }
    async fn put_blob(&self, bytes: Bytes) -> Result<ObjectRef> {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[index].lock().await.execute("INSERT INTO loom_blobs(namespace,digest,body) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", &[&self.namespace,&digest,&bytes]).await.map_err(db)?;
        Ok(ObjectRef::new(digest))
    }
    async fn get_blob(&self, object: &ObjectRef) -> Result<Bytes> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let row = self.clients[index]
            .lock()
            .await
            .query_opt(
                "SELECT body FROM loom_blobs WHERE namespace=$1 AND digest=$2",
                &[&self.namespace, &object.digest],
            )
            .await
            .map_err(db)?;
        let bytes: Bytes = row
            .ok_or_else(|| Error::NotFound(object.digest.clone()))?
            .get(0);
        if format!("{:x}", Sha256::digest(&bytes)) != object.digest {
            return Err(Error::Invalid("blob integrity failure".into()));
        }
        Ok(bytes)
    }
    async fn blob_size(&self, object: &ObjectRef) -> Result<u64> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let row = self.clients[index]
            .lock()
            .await
            .query_opt(
                "SELECT octet_length(body) FROM loom_blobs WHERE namespace=$1 AND digest=$2",
                &[&self.namespace, &object.digest],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| Error::NotFound(object.digest.clone()))?;
        Ok(row.get::<_, i32>(0) as u64)
    }
    async fn get_blob_range(&self, object: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::Invalid("object range overflow".into()))?;
        // PostgreSQL bytea is bounded by a signed 32-bit length.
        let start = i32::try_from(offset)
            .ok()
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| Error::Invalid("object offset exceeds PostgreSQL limit".into()))?;
        let count = i32::try_from(length)
            .map_err(|_| Error::Invalid("object range exceeds PostgreSQL limit".into()))?;
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let row = self.clients[index].lock().await.query_opt(
            "SELECT octet_length(body),substring(body FROM $3 FOR $4) FROM loom_blobs WHERE namespace=$1 AND digest=$2",
            &[&self.namespace, &object.digest, &start, &count]).await.map_err(db)?;
        let row = row.ok_or_else(|| Error::NotFound(object.digest.clone()))?;
        let size: i32 = row.get(0);
        if end > size as u64 {
            return Err(Error::Invalid("object range exceeds length".into()));
        }
        Ok(row.get(1))
    }
}
