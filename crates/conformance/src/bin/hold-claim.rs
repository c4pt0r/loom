//! Test fixture, not a production worker: claim then wait so SIGKILL is deterministic.
use loom_contracts::*;
use std::io::Write;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("LOOM_DATABASE_URL")?;
    let ns = std::env::args().nth(1).ok_or("namespace required")?;
    let backend = loom_backend_postgres::connect(&url, &ns).await?;
    let session = NodeSession {
        node: "crash-fixture".into(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    };
    backend.register(session.clone(), 1000).await?;
    let claim = backend.claim(&session, 1000).await?.ok_or("no work")?;
    println!("{}", claim.fence.job);
    std::io::stdout().flush()?;
    std::future::pending::<()>().await;
    Ok(())
}
