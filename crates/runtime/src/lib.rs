//! Execution orchestration. Depends only on contracts, never on storage adapters.
use loom_contracts::*;
use std::{sync::Arc, time::Duration};

pub struct Node {
    pub store: Arc<dyn Backend>,
    pub engine: Arc<dyn GuestEngine>,
    pub session: NodeSession,
    pub lease_ms: Millis,
}
impl Node {
    pub async fn register(&self) -> Result<()> {
        self.store
            .register(self.session.clone(), self.lease_ms)
            .await
    }
    pub async fn execute(&self, claim: &Claim) -> Result<Completion> {
        match &claim.work {
            Work::Task { spec, input } => {
                let input = match input {
                    Input::Inline(bytes) => bytes.clone(),
                    Input::Object(r) => self.store.get(r).await?,
                    Input::TaskResult(_) => {
                        return Err(Error::Invalid("unresolved task dependency".into()));
                    }
                };
                let bytes = self
                    .engine
                    .task(&spec.module, &spec.function, input)
                    .await?;
                // Publish the blob before committing its reference. Failed commit may leave an orphan.
                let output = self.store.put(bytes).await?;
                Ok(Completion::Task { output })
            }
            Work::Actor { snapshot, message } => {
                let decision = self
                    .engine
                    .actor(
                        &snapshot.spec.module,
                        &snapshot.spec.handler,
                        snapshot.state.clone(),
                        message.payload.clone(),
                    )
                    .await?;
                Ok(Completion::Actor {
                    expected_revision: snapshot.revision,
                    decision,
                })
            }
        }
    }
    pub async fn tick(&self) -> Result<bool> {
        let Some(claim) = self.store.claim(&self.session, self.lease_ms).await? else {
            return Ok(false);
        };
        eprintln!(
            "CLAIM {} epoch={} attempt={}",
            claim.fence.job, claim.fence.epoch, claim.attempt
        );
        let execution = self.execute(&claim);
        tokio::pin!(execution);
        // Poll BOTH futures while either awaits the backend. Awaiting renewal inside
        // a select branch can deadlock if execute currently owns a connection mutex.
        let renewal = async {
            loop {
                tokio::time::sleep(Duration::from_millis((self.lease_ms / 3).max(1))).await;
                self.store.renew_claim(&claim, self.lease_ms).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        };
        tokio::pin!(renewal);
        let result = tokio::select! {
            result = &mut execution => result,
            lost = &mut renewal => { lost?; return Err(Error::Fenced); }
        };
        match result {
            Ok(result) => {
                // Do not turn an unknown commit outcome into failure/re-execution.
                match self.store.commit(&claim, result).await {
                    Ok(_) => eprintln!("COMMIT {}", claim.fence.job),
                    Err(error @ (Error::Invalid(_) | Error::NotFound(_) | Error::Conflict(_))) => {
                        self.store.fail(&claim, error.to_string()).await?;
                        eprintln!("RESTART {}: {error}", claim.fence.job);
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => {
                self.store.fail(&claim, error.to_string()).await?;
                eprintln!("RESTART {}: {error}", claim.fence.job);
            }
        }
        Ok(true)
    }
    /// Heartbeats run independently of Wasm execution. On failure stop dispatching;
    /// a bounded in-flight guest may finish, but commits still require a valid lease.
    pub async fn run(&self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        self.register().await?;
        let store = self.store.clone();
        let session = self.session.clone();
        let ttl = self.lease_ms;
        let (lost_tx, mut lost_rx) = tokio::sync::oneshot::channel();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis((ttl / 3).max(1))).await;
                if let Err(e) = store.renew(&session, ttl).await {
                    let _ = lost_tx.send(e);
                    break;
                }
            }
        });
        tokio::pin!(shutdown);
        let result = loop {
            let tick = self.tick();
            tokio::pin!(tick);
            tokio::select! {
                _ = &mut shutdown => {
                    let (drained, finished) = tokio::join!(self.store.drain(&self.session), &mut tick);
                    break drained.and(finished.map(|_| ()));
                }
                lost = &mut lost_rx => { break Err(lost.unwrap_or(Error::Fenced)); }
                result = &mut tick => {
                    match result {
                        Ok(false) => tokio::time::sleep(Duration::from_millis(50)).await,
                        Ok(true) => (),
                        Err(e) => break Err(e),
                    }
                }
            }
        };
        heartbeat.abort();
        result
    }
}
