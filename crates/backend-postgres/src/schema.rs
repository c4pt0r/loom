use super::*;

pub async fn initialize(client: &mut Client, namespace: &str) -> Result<()> {
    // Established namespaces do not run DDL or take bootstrap locks on every connection.
    let exists: bool = client
        .query_one("SELECT to_regclass('loom_ledgers') IS NOT NULL", &[])
        .await
        .map_err(db)?
        .get(0);
    if exists
        && let Some(row) = client
            .query_opt(
                "SELECT schema_version FROM loom_ledgers WHERE namespace=$1",
                &[&namespace],
            )
            .await
            .map_err(db)?
    {
        match row.get::<_, i32>(0) {
            2 => return Ok(()),
            1 => (),
            _ => return Err(Error::Invalid("unsupported coordination schema".into())),
        }
    }
    let tx = client.transaction().await.map_err(db)?;
    // Bootstrap only: no namespace-wide lock participates in normal operations.
    tx.batch_execute("SELECT pg_advisory_xact_lock(781246201);
        CREATE TABLE IF NOT EXISTS loom_ledgers(namespace text PRIMARY KEY, schema_version integer NOT NULL, body text NOT NULL);
        CREATE TABLE IF NOT EXISTS loom_blobs(namespace text NOT NULL, digest text NOT NULL, body bytea NOT NULL, PRIMARY KEY(namespace,digest));
        CREATE SEQUENCE IF NOT EXISTS loom_job_sequence;
        CREATE TABLE IF NOT EXISTS loom_nodes(namespace text NOT NULL,id text NOT NULL,incarnation text NOT NULL,lease_until bigint NOT NULL,body text NOT NULL,PRIMARY KEY(namespace,id));
        CREATE TABLE IF NOT EXISTS loom_actors(namespace text NOT NULL,id text NOT NULL,body text NOT NULL,PRIMARY KEY(namespace,id));
        CREATE TABLE IF NOT EXISTS loom_jobs(namespace text NOT NULL,id text NOT NULL,seq bigint NOT NULL,actor text,owner text,incarnation text,lease_until bigint NOT NULL,available_at bigint NOT NULL,attempts integer NOT NULL,done boolean NOT NULL,failed boolean NOT NULL,body text NOT NULL,PRIMARY KEY(namespace,id));
        CREATE INDEX IF NOT EXISTS loom_jobs_ready ON loom_jobs(namespace,seq) WHERE NOT done;
        CREATE INDEX IF NOT EXISTS loom_jobs_actor ON loom_jobs(namespace,actor,seq) WHERE NOT done;
        CREATE TABLE IF NOT EXISTS loom_dependencies(namespace text NOT NULL,job text NOT NULL,parent text NOT NULL,PRIMARY KEY(namespace,job,parent));
    ").await.map_err(db)?;
    tx.execute("INSERT INTO loom_ledgers(namespace,schema_version,body) VALUES($1,2,'{}') ON CONFLICT DO NOTHING", &[&namespace]).await.map_err(db)?;
    let row = tx
        .query_one(
            "SELECT schema_version,body FROM loom_ledgers WHERE namespace=$1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(db)?;
    match row.get::<_, i32>(0) {
        2 => (),
        1 => {
            let mut ledger: Ledger = decode(row.get(1))?;
            let adapter = PostgresDriver {
                clients: vec![],
                next: AtomicUsize::new(0),
                namespace: namespace.into(),
            };
            adapter.save(&tx, &Ledger::default(), &mut ledger).await?;
            // Preserve the old ledger as a migration snapshot; old workers now fail closed.
            tx.execute(
                "UPDATE loom_ledgers SET schema_version=2 WHERE namespace=$1",
                &[&namespace],
            )
            .await
            .map_err(db)?;
        }
        _ => return Err(Error::Invalid("unsupported coordination schema".into())),
    }
    tx.commit().await.map_err(db)
}
