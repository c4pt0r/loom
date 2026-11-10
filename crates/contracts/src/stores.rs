//! Compose a coordination backend and an independent payload store.
use crate::*;
use std::sync::Arc;

#[derive(Clone)]
pub struct Stores {
    control: Arc<dyn ControlStore>,
    objects: Arc<dyn ObjectStore>,
}
impl Stores {
    pub fn new(control: Arc<dyn ControlStore>, objects: Arc<dyn ObjectStore>) -> Self {
        Self { control, objects }
    }
    pub fn control(&self) -> Arc<dyn ControlStore> {
        self.control.clone()
    }
    pub fn objects(&self) -> Arc<dyn ObjectStore> {
        self.objects.clone()
    }
}
#[async_trait]
impl Membership for Stores {
    async fn register(&self, s: NodeSession, ttl: Millis) -> Result<()> {
        self.control.register(s, ttl).await
    }
    async fn renew(&self, s: &NodeSession, ttl: Millis) -> Result<()> {
        self.control.renew(s, ttl).await
    }
    async fn drain(&self, s: &NodeSession) -> Result<()> {
        self.control.drain(s).await
    }
    async fn members(&self) -> Result<Vec<NodeRecord>> {
        self.control.members().await
    }
}
#[async_trait]
impl ExecutionStore for Stores {
    async fn create_actor(&self, s: ActorSpec) -> Result<()> {
        self.control.create_actor(s).await
    }
    async fn actor(&self, id: &str) -> Result<ActorSnapshot> {
        self.control.actor(id).await
    }
    async fn send(&self, e: Envelope) -> Result<()> {
        self.control.send(e).await
    }
    async fn submit(&self, s: TaskSpec) -> Result<()> {
        self.control.submit(s).await
    }
    async fn task(&self, id: &str) -> Result<TaskStatus> {
        self.control.task(id).await
    }
    async fn claim(&self, owner: &NodeSession, ttl: Millis) -> Result<Option<Claim>> {
        self.control.claim(owner, ttl).await
    }
    async fn renew_claim(&self, c: &Claim, ttl: Millis) -> Result<()> {
        self.control.renew_claim(c, ttl).await
    }
    async fn commit(&self, c: &Claim, completion: Completion) -> Result<Receipt> {
        self.control.commit(c, completion).await
    }
    async fn fail(&self, c: &Claim, reason: String) -> Result<()> {
        self.control.fail(c, reason).await
    }
    async fn receipt(&self, id: &str) -> Result<Option<Receipt>> {
        self.control.receipt(id).await
    }
}
#[async_trait]
impl ObjectStore for Stores {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef> {
        self.objects.put(bytes).await
    }
    async fn get(&self, object: &ObjectRef) -> Result<Bytes> {
        self.objects.get(object).await
    }
    async fn size(&self, object: &ObjectRef) -> Result<u64> {
        self.objects.size(object).await
    }
    async fn get_range(&self, object: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        self.objects.get_range(object, offset, length).await
    }
}
