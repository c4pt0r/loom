//! Reference adapter: one locked ledger row per namespace, separate immutable blobs.
//! Intentionally prioritizes contract validation over throughput. Local TLS-free prototype.
use async_trait::async_trait;
use loom_backend_core::{Answer, Driver, Ledger, Op, Store};
use loom_contracts::*;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

pub struct PostgresDriver {
    client: Mutex<Client>,
    namespace: String,
}
pub type PostgresStore = Store<PostgresDriver>;
fn unavailable(e: impl std::fmt::Display) -> Error {
    Error::Unavailable(e.to_string())
}

pub async fn connect(url: &str, namespace: &str) -> Result<PostgresStore> {
    if namespace.is_empty() {
        return Err(Error::Invalid("namespace required".into()));
    }
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(unavailable)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection closed: {e}");
        }
    });
    // Advisory lock serializes concurrent first-node bootstrap DDL.
    client.batch_execute("SELECT pg_advisory_lock(781246201);
        CREATE TABLE IF NOT EXISTS loom_ledgers(namespace text PRIMARY KEY, schema_version integer NOT NULL, body text NOT NULL);
        CREATE TABLE IF NOT EXISTS loom_blobs(namespace text NOT NULL, digest text NOT NULL, body bytea NOT NULL, PRIMARY KEY(namespace,digest));
        SELECT pg_advisory_unlock(781246201);").await.map_err(unavailable)?;
    let body = serde_json::to_string(&Ledger::default()).map_err(unavailable)?;
    client.execute("INSERT INTO loom_ledgers(namespace,schema_version,body) VALUES($1,1,$2) ON CONFLICT DO NOTHING", &[&namespace, &body]).await.map_err(unavailable)?;
    Ok(Store(PostgresDriver {
        client: Mutex::new(client),
        namespace: namespace.into(),
    }))
}

#[async_trait]
impl Driver for PostgresDriver {
    async fn transact(&self, op: Op) -> Result<Answer> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(unavailable)?;
        let row = tx
            .query_one(
                "SELECT schema_version,body FROM loom_ledgers WHERE namespace=$1 FOR UPDATE",
                &[&self.namespace],
            )
            .await
            .map_err(unavailable)?;
        if row.get::<_, i32>(0) != 1 {
            return Err(Error::Invalid("unsupported ledger schema".into()));
        }
        let body: String = row.get(1);
        let mut ledger: Ledger = serde_json::from_str(&body).map_err(unavailable)?;
        // Read clock AFTER acquiring the row lock, not transaction-start time.
        let time = tx
            .query_one(
                "SELECT floor(extract(epoch from clock_timestamp())*1000)::bigint",
                &[],
            )
            .await
            .map_err(unavailable)?;
        let now: i64 = time.get(0);
        let answer = ledger.apply(op, now as u64)?;
        let next = serde_json::to_string(&ledger).map_err(unavailable)?;
        if next != body {
            tx.execute(
                "UPDATE loom_ledgers SET body=$2 WHERE namespace=$1",
                &[&self.namespace, &next],
            )
            .await
            .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(answer)
    }
    async fn put_blob(&self, bytes: Bytes) -> Result<ObjectRef> {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        self.client.lock().await.execute("INSERT INTO loom_blobs(namespace,digest,body) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            &[&self.namespace, &digest, &bytes]).await.map_err(unavailable)?;
        Ok(ObjectRef::new(digest))
    }
    async fn get_blob(&self, object: &ObjectRef) -> Result<Bytes> {
        let row = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT body FROM loom_blobs WHERE namespace=$1 AND digest=$2",
                &[&self.namespace, &object.digest],
            )
            .await
            .map_err(unavailable)?;
        let bytes: Bytes = row
            .ok_or_else(|| Error::NotFound(object.digest.clone()))?
            .get(0);
        if format!("{:x}", Sha256::digest(&bytes)) != object.digest {
            return Err(Error::Invalid("blob integrity failure".into()));
        }
        Ok(bytes)
    }
}
