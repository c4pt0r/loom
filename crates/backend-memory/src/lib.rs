//! Volatile adapter for development and deterministic contract testing.
use async_trait::async_trait;
use loom_backend_core::{Answer, Driver, Ledger, Op, Store};
use loom_contracts::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> Millis;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> Millis {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
#[derive(Default)]
pub struct ManualClock(pub AtomicU64);
impl ManualClock {
    pub fn advance(&self, ms: Millis) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}
impl Clock for ManualClock {
    fn now_ms(&self) -> Millis {
        self.0.load(Ordering::SeqCst)
    }
}

pub struct MemoryDriver {
    ledger: Mutex<Ledger>,
    blobs: Mutex<BTreeMap<String, Bytes>>,
    clock: Arc<dyn Clock>,
}
pub type MemoryStore = Store<MemoryDriver>;
pub fn memory(clock: Arc<dyn Clock>) -> MemoryStore {
    Store(MemoryDriver {
        ledger: Mutex::new(Ledger::default()),
        blobs: Mutex::default(),
        clock,
    })
}
#[async_trait]
impl Driver for MemoryDriver {
    async fn transact(&self, op: Op) -> Result<Answer> {
        let mut ledger = self.ledger.lock().unwrap();
        let mut next = ledger.clone();
        let answer = next.apply(op, self.clock.now_ms())?;
        *ledger = next;
        Ok(answer)
    }
    async fn put_blob(&self, bytes: Bytes) -> Result<ObjectRef> {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        self.blobs
            .lock()
            .unwrap()
            .entry(digest.clone())
            .or_insert(bytes);
        Ok(ObjectRef::new(digest))
    }
    async fn get_blob(&self, object: &ObjectRef) -> Result<Bytes> {
        self.blobs
            .lock()
            .unwrap()
            .get(&object.digest)
            .cloned()
            .ok_or_else(|| Error::NotFound(object.digest.clone()))
    }
}
