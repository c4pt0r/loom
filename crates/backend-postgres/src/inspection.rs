//! Read-only, bounded page reads. No execution locks or namespace-wide ledger load.
use super::*;
use loom_backend_core::inspection::{InspectionDriver, actor_view, detail, job_view, node_status};
use tokio_postgres::IsolationLevel;

// Mirrors the reference projection. Ownership requires both leases and the current incarnation.
const CLASSIFIED: &str = r#"WITH classified AS (
 SELECT j.*, CASE
 WHEN j.done AND j.failed THEN 'failed'
 WHEN j.done THEN 'succeeded'
 WHEN j.lease_until>$2 AND n.lease_until>$2 AND j.incarnation=n.incarnation THEN 'running'
 WHEN j.attempts>0 THEN 'retrying'
 WHEN j.available_at>$2 THEN 'waiting'
 WHEN j.actor IS NULL AND EXISTS (
   SELECT 1 FROM loom_dependencies d LEFT JOIN loom_jobs p ON p.namespace=d.namespace AND p.id=d.parent
   WHERE d.namespace=j.namespace AND d.job=j.id AND (p.id IS NULL OR NOT p.done OR p.failed)) THEN 'waiting'
 WHEN j.actor IS NOT NULL AND EXISTS (
   SELECT 1 FROM loom_jobs p LEFT JOIN loom_nodes pn ON pn.namespace=p.namespace AND pn.id=p.owner
   WHERE p.namespace=j.namespace AND p.actor=j.actor AND p.id<>j.id AND NOT p.done AND
   ((p.lease_until>$2 AND pn.lease_until>$2 AND p.incarnation=pn.incarnation)
    OR (p.seq<j.seq AND (p.available_at<=$2 OR p.attempts>0)))) THEN 'waiting'
 ELSE 'queued' END AS phase
 FROM loom_jobs j LEFT JOIN loom_nodes n ON n.namespace=j.namespace AND n.id=j.owner
 WHERE j.namespace=$1
) "#;
const FILTER: &str = r#" FROM classified WHERE (actor IS NULL)=$3
 AND ($4='' OR strpos(lower(id || ' ' || COALESCE(body::jsonb#>>'{kind,Task,function}','Actor message') || ' ' || COALESCE(actor,body::jsonb#>>'{kind,Task,notify}','')),$4)>0)
 AND ($5='' OR phase=$5) "#;
fn job_phase(s: &str) -> Result<JobPhase> {
    decode(&format!("\"{s}\""))
}
impl PostgresDriver {
    async fn projected_job(
        &self,
        tx: &Transaction<'_>,
        id: &str,
        body: &str,
        status: &str,
    ) -> Result<(JobView, Job, ObjectRef)> {
        let j: Job = decode(body)?;
        let (max, module) = match &j.kind {
            Kind::Task(t) => (t.max_attempts, t.module.clone()),
            Kind::Message(m) => {
                let row = tx
                    .query_one(
                        "SELECT body FROM loom_actors WHERE namespace=$1 AND id=$2",
                        &[&self.namespace, &m.actor],
                    )
                    .await
                    .map_err(db)?;
                let a: ActorSnapshot = decode(row.get(0))?;
                (a.spec.supervisor.max_attempts, a.spec.module)
            }
        };
        Ok((job_view(id, &j, job_phase(status)?, max), j, module))
    }
    async fn projected_actor(
        &self,
        tx: &Transaction<'_>,
        a: ActorSnapshot,
        time: i64,
        limit: usize,
    ) -> Result<ActorView> {
        let row=tx.query_one("SELECT count(*),count(*) FILTER (WHERE j.lease_until>$3 AND n.lease_until>$3 AND j.incarnation=n.incarnation) FROM loom_jobs j LEFT JOIN loom_nodes n ON n.namespace=j.namespace AND n.id=j.owner WHERE j.namespace=$1 AND j.actor=$2 AND NOT j.done",&[&self.namespace,&a.spec.id,&time]).await.map_err(db)?;
        let outstanding = row.get::<_, i64>(0) as u64;
        let running = row.get::<_, i64>(1) as u64;
        Ok(actor_view(
            &a,
            outstanding.saturating_sub(running),
            running,
            limit,
        ))
    }
}
#[async_trait]
impl InspectionDriver for PostgresDriver {
    async fn inspect(&self, q: InspectQuery) -> Result<Inspection> {
        q.validate()?;
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let mut client = self.clients[index].lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(db)?;
        tx.batch_execute("SET LOCAL statement_timeout = '5s'")
            .await
            .map_err(db)?;
        let time = now(&tx).await? as i64;
        let mut result = Inspection {
            now_ms: time as u64,
            summary: InspectSummary::default(),
            total: 0,
            jobs: vec![],
            nodes: vec![],
            actors: vec![],
        };
        for row in tx
            .query(
                &format!(
                    "{CLASSIFIED} SELECT actor IS NULL,phase,count(*) FROM classified GROUP BY 1,2"
                ),
                &[&self.namespace, &time],
            )
            .await
            .map_err(db)?
        {
            let counts = if row.get::<_, bool>(0) {
                &mut result.summary.tasks
            } else {
                &mut result.summary.messages
            };
            counts.insert(row.get(1), row.get::<_, i64>(2) as u64);
        }
        for row in tx.query("SELECT CASE WHEN lease_until<=$2 THEN 'expired' WHEN (body::jsonb->>'draining')::boolean THEN 'draining' ELSE 'online' END,count(*) FROM loom_nodes WHERE namespace=$1 GROUP BY 1",&[&self.namespace,&time]).await.map_err(db)? { result.summary.nodes.insert(row.get(0),row.get::<_,i64>(1) as u64); }
        result.summary.actors = tx
            .query_one(
                "SELECT count(*) FROM loom_actors WHERE namespace=$1",
                &[&self.namespace],
            )
            .await
            .map_err(db)?
            .get::<_, i64>(0) as u64;
        let search = q.search.to_lowercase();
        let limit = q.limit as i64;
        let offset = q.offset as i64;
        match q.resource {
            InspectResource::Tasks | InspectResource::Messages => {
                let task = q.resource == InspectResource::Tasks;
                let status = q.phase.map(JobPhase::as_str).unwrap_or("");
                result.total = tx
                    .query_one(
                        &format!("{CLASSIFIED} SELECT count(*) {FILTER}"),
                        &[&self.namespace, &time, &task, &search, &status],
                    )
                    .await
                    .map_err(db)?
                    .get::<_, i64>(0) as u64;
                for row in tx.query(&format!("{CLASSIFIED} SELECT id,body,phase {FILTER} ORDER BY seq DESC LIMIT $6 OFFSET $7"),&[&self.namespace,&time,&task,&search,&status,&limit,&offset]).await.map_err(db)? {
                    result.jobs.push(self.projected_job(&tx,row.get(0),row.get(1),row.get(2)).await?.0);
                }
            }
            InspectResource::Actors => {
                let filter = " FROM loom_actors WHERE namespace=$1 AND ($2='' OR strpos(lower(id || ' ' || (body::jsonb#>>'{spec,handler}')),$2)>0)";
                result.total = tx
                    .query_one(
                        &format!("SELECT count(*) {filter}"),
                        &[&self.namespace, &search],
                    )
                    .await
                    .map_err(db)?
                    .get::<_, i64>(0) as u64;
                for row in tx
                    .query(
                        &format!("SELECT body {filter} ORDER BY id LIMIT $3 OFFSET $4"),
                        &[&self.namespace, &search, &limit, &offset],
                    )
                    .await
                    .map_err(db)?
                {
                    result.actors.push(
                        self.projected_actor(&tx, decode(row.get(0))?, time, 2048)
                            .await?,
                    );
                }
            }
            InspectResource::Nodes => {
                let filter =
                    " FROM loom_nodes WHERE namespace=$1 AND ($2='' OR strpos(lower(id),$2)>0)";
                result.total = tx
                    .query_one(
                        &format!("SELECT count(*) {filter}"),
                        &[&self.namespace, &search],
                    )
                    .await
                    .map_err(db)?
                    .get::<_, i64>(0) as u64;
                for row in tx
                    .query(
                        &format!("SELECT body {filter} ORDER BY id LIMIT $3 OFFSET $4"),
                        &[&self.namespace, &search, &limit, &offset],
                    )
                    .await
                    .map_err(db)?
                {
                    let n: NodeRecord = decode(row.get(0))?;
                    let running=tx.query_one("SELECT count(*) FROM loom_jobs WHERE namespace=$1 AND owner=$2 AND incarnation=$3 AND lease_until>$4 AND NOT done",&[&self.namespace,&n.session.node,&n.session.incarnation,&time]).await.map_err(db)?.get::<_,i64>(0) as u64;
                    result.nodes.push(NodeView {
                        status: node_status(&n, time as u64).into(),
                        lease_until: n.lease_until,
                        running: if n.lease_until > time as u64 {
                            running
                        } else {
                            0
                        },
                        session: n.session,
                    });
                }
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    async fn inspect_job(&self, id: &str) -> Result<JobDetail> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let mut client = self.clients[index].lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(db)?;
        tx.batch_execute("SET LOCAL statement_timeout = '5s'")
            .await
            .map_err(db)?;
        let time = now(&tx).await? as i64;
        let row = tx
            .query_opt(
                &format!("{CLASSIFIED} SELECT body,phase FROM classified WHERE id=$3"),
                &[&self.namespace, &time, &id],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| Error::NotFound(id.into()))?;
        let (view, j, module) = self.projected_job(&tx, id, row.get(0), row.get(1)).await?;
        let result = detail(view, &j, module)?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
    async fn inspect_actor(&self, id: &str) -> Result<ActorView> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let mut client = self.clients[index].lock().await;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(db)?;
        tx.batch_execute("SET LOCAL statement_timeout = '5s'")
            .await
            .map_err(db)?;
        let time = now(&tx).await? as i64;
        let row = tx
            .query_opt(
                "SELECT body FROM loom_actors WHERE namespace=$1 AND id=$2",
                &[&self.namespace, &id],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| Error::NotFound(id.into()))?;
        let result = self
            .projected_actor(&tx, decode(row.get(0))?, time, 65536)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
}
