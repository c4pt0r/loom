//! Liveness regression: publication and lease renewal contend for one connection.
use async_trait::async_trait;
use loom_contracts::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

struct SlowConnection {
    inner: Arc<dyn Backend>,
    connection: Mutex<()>,
}
#[async_trait]
impl Membership for SlowConnection {
    async fn register(&self, s: NodeSession, t: Millis) -> Result<()> {
        self.inner.register(s, t).await
    }
    async fn renew(&self, s: &NodeSession, t: Millis) -> Result<()> {
        self.inner.renew(s, t).await
    }
    async fn drain(&self, s: &NodeSession) -> Result<()> {
        self.inner.drain(s).await
    }
    async fn members(&self) -> Result<Vec<NodeRecord>> {
        self.inner.members().await
    }
}
#[async_trait]
impl ExecutionStore for SlowConnection {
    async fn create_actor(&self, s: ActorSpec) -> Result<()> {
        self.inner.create_actor(s).await
    }
    async fn actor(&self, id: &str) -> Result<ActorSnapshot> {
        self.inner.actor(id).await
    }
    async fn send(&self, e: Envelope) -> Result<()> {
        self.inner.send(e).await
    }
    async fn submit(&self, s: TaskSpec) -> Result<()> {
        self.inner.submit(s).await
    }
    async fn task(&self, id: &str) -> Result<TaskStatus> {
        self.inner.task(id).await
    }
    async fn claim(&self, s: &NodeSession, t: Millis) -> Result<Option<Claim>> {
        self.inner.claim(s, t).await
    }
    async fn renew_claim(&self, c: &Claim, t: Millis) -> Result<()> {
        let _connection = self.connection.lock().await;
        self.inner.renew_claim(c, t).await
    }
    async fn commit(&self, c: &Claim, x: Completion) -> Result<Receipt> {
        self.inner.commit(c, x).await
    }
    async fn fail(&self, c: &Claim, e: String) -> Result<()> {
        self.inner.fail(c, e).await
    }
    async fn receipt(&self, id: &str) -> Result<Option<Receipt>> {
        self.inner.receipt(id).await
    }
}
#[async_trait]
impl ObjectStore for SlowConnection {
    async fn put(&self, b: Bytes) -> Result<ObjectRef> {
        let _connection = self.connection.lock().await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        self.inner.put(b).await
    }
    async fn get(&self, r: &ObjectRef) -> Result<Bytes> {
        self.inner.get(r).await
    }
}
struct Echo;
#[async_trait]
impl GuestEngine for Echo {
    async fn task(&self, _: &ObjectRef, _: &str, bytes: Bytes) -> Result<Bytes> {
        Ok(bytes)
    }
    async fn actor(&self, _: &ObjectRef, _: &str, _: Bytes, _: Bytes) -> Result<Decision> {
        unreachable!()
    }
}

#[tokio::test]
async fn publication_and_renewal_must_progress_concurrently() {
    let backend: Arc<dyn Backend> = Arc::new(SlowConnection {
        inner: Arc::new(loom_backend_memory::memory(Arc::new(
            loom_backend_memory::ManualClock::default(),
        ))),
        connection: Mutex::new(()),
    });
    backend
        .submit(TaskSpec {
            id: "slow-put".into(),
            module: ObjectRef::new("mock".into()),
            function: "echo".into(),
            input: Input::Inline(b"result".to_vec()),
            output: loom_contracts::TaskOutput::Value,
            max_attempts: 1,
            notify: None,
        })
        .await
        .unwrap();
    let node = loom_runtime::Node {
        store: backend.clone(),
        engine: Arc::new(Echo),
        session: NodeSession {
            node: "test".into(),
            incarnation: "boot".into(),
        },
        lease_ms: 90,
    };
    node.register().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), node.tick())
            .await
            .expect("renewal deadlocked publication")
            .unwrap()
    );
    assert!(matches!(
        backend.task("slow-put").await.unwrap().outcome,
        Some(TaskOutcome::Succeeded(_))
    ));
}
