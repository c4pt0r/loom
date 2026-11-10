use async_trait::async_trait;
use loom_contracts::*;
use loom_object_store_memory::MemoryObjectStore;
use loom_runtime::Node;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Echo;
#[async_trait]
impl GuestEngine for Echo {
    async fn task(&self, _: &ObjectRef, _: &str, input: Bytes) -> Result<Bytes> {
        Ok(input)
    }
    async fn actor(&self, _: &ObjectRef, _: &str, _: Bytes, _: Bytes) -> Result<Decision> {
        unreachable!()
    }
}
struct RangeOnly {
    reads: AtomicUsize,
}
#[async_trait]
impl ObjectStore for RangeOnly {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef> {
        assert_eq!(bytes, b"piece");
        Ok(ObjectRef::new("piece".into()))
    }
    async fn get(&self, _: &ObjectRef) -> Result<Bytes> {
        panic!("runtime downloaded the whole source")
    }
    async fn get_range(&self, _: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        assert_eq!((offset, length), (6, 5));
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(b"piece".to_vec())
    }
}
fn task(id: &str, input: Input) -> TaskSpec {
    TaskSpec {
        id: id.into(),
        module: ObjectRef::new("module".into()),
        function: "echo".into(),
        input,
        output: TaskOutput::Value,
        max_attempts: 1,
        notify: None,
    }
}
#[tokio::test]
async fn slice_execution_uses_range_io_and_whole_object_inputs_remain_compatible() {
    let control = Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::ManualClock::default(),
    )));
    let objects = Arc::new(RangeOnly {
        reads: AtomicUsize::new(0),
    });
    let store: Arc<dyn Backend> = Arc::new(Stores::new(control.clone(), objects.clone()));
    let node = Node {
        store: store.clone(),
        engine: Arc::new(Echo),
        session: NodeSession {
            node: "worker".into(),
            incarnation: "boot".into(),
        },
        lease_ms: 5000,
    };
    node.register().await.unwrap();
    let input = Input::ObjectSlice(ObjectSlice {
        object: ObjectRef::new("source".into()),
        offset: 6,
        length: 5,
    });
    let restored: Input = serde_json::from_slice(&serde_json::to_vec(&input).unwrap()).unwrap();
    store.submit(task("slice", restored)).await.unwrap();
    node.tick().await.unwrap();
    assert_eq!(objects.reads.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.task("slice").await.unwrap().outcome,
        Some(TaskOutcome::Succeeded(_))
    ));
    let legacy = Arc::new(MemoryObjectStore::default());
    let whole = legacy.put(b"complete payload".to_vec()).await.unwrap();
    let store: Arc<dyn Backend> = Arc::new(Stores::new(control, legacy.clone()));
    let node = Node {
        store: store.clone(),
        ..node
    };
    store
        .submit(task("whole", Input::Object(whole.clone())))
        .await
        .unwrap();
    node.tick().await.unwrap();
    let Some(TaskOutcome::Succeeded(result)) = store.task("whole").await.unwrap().outcome else {
        panic!()
    };
    assert_eq!(
        legacy.get(&result).await.unwrap(),
        legacy.get(&whole).await.unwrap()
    );
}
struct LegacyStore;
#[async_trait]
impl ObjectStore for LegacyStore {
    async fn put(&self, _: Bytes) -> Result<ObjectRef> {
        unreachable!()
    }
    async fn get(&self, _: &ObjectRef) -> Result<Bytes> {
        Ok(b"abcdef".to_vec())
    }
}
#[tokio::test]
async fn range_contract_fallback_and_bounds() {
    let object = ObjectRef::new("source".into());
    assert_eq!(LegacyStore.get_range(&object, 1, 3).await.unwrap(), b"bcd");
    assert!(
        LegacyStore
            .get_range(&object, 6, 0)
            .await
            .unwrap()
            .is_empty()
    );
    for (offset, length) in [(7, 0), (5, 2), (u64::MAX, 1)] {
        assert!(
            LegacyStore
                .get_range(&object, offset, length)
                .await
                .is_err()
        );
    }
    let memory = MemoryObjectStore::default();
    let reference = memory.put(b"a\0\xffb".to_vec()).await.unwrap();
    assert_eq!(
        memory.get_range(&reference, 1, 2).await.unwrap(),
        vec![0, 255]
    );
    assert!(
        memory
            .get_range(&ObjectRef::new("missing".into()), 0, 0)
            .await
            .is_err()
    );
    let store = loom_backend_memory::memory(Arc::new(loom_backend_memory::ManualClock::default()));
    assert!(
        store
            .submit(task(
                "overflow",
                Input::ObjectSlice(ObjectSlice {
                    object,
                    offset: u64::MAX,
                    length: 1
                })
            ))
            .await
            .is_err()
    );
}
