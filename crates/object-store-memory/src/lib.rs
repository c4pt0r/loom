//! An object-only adapter for tests and single-process development.
//! Each instance is a logical namespace. Share its Arc, or clone it, to share bytes.
//! This store is volatile and cannot be shared between separate worker processes.
use async_trait::async_trait;
use loom_contracts::{Bytes, Error, ObjectRef, ObjectStore, Result, slice_bytes};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub struct MemoryObjectStore {
    objects: Arc<Mutex<HashMap<String, Bytes>>>,
}
#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef> {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        self.objects
            .lock()
            .unwrap()
            .entry(digest.clone())
            .or_insert(bytes);
        Ok(ObjectRef::new(digest))
    }
    async fn get(&self, object: &ObjectRef) -> Result<Bytes> {
        self.objects
            .lock()
            .unwrap()
            .get(&object.digest)
            .cloned()
            .ok_or_else(|| Error::NotFound(object.digest.clone()))
    }
    async fn get_range(&self, object: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        let objects = self.objects.lock().unwrap();
        let bytes = objects
            .get(&object.digest)
            .ok_or_else(|| Error::NotFound(object.digest.clone()))?;
        slice_bytes(bytes, offset, length)
    }
}
