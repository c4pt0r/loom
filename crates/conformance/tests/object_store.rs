use async_trait::async_trait;
use loom_contracts::*;
use loom_object_store_memory::MemoryObjectStore;
use loom_runtime::Node;
use std::sync::Arc;

struct Sum;
#[async_trait]
impl GuestEngine for Sum {
    async fn task(&self, _: &ObjectRef, name: &str, bytes: Bytes) -> Result<Bytes> {
        if name == "map" {
            return Ok(bytes);
        }
        let frame: ReduceFrame = loom_shuffle::decode(&bytes)?;
        let mut sum: u64 = frame
            .state
            .as_deref()
            .map(loom_shuffle::decode)
            .transpose()?
            .unwrap_or(0);
        for record in frame.records {
            sum += loom_shuffle::decode::<u64>(&record.value)?;
        }
        loom_shuffle::encode(&sum)
    }
    async fn actor(&self, _: &ObjectRef, _: &str, _: Bytes, _: Bytes) -> Result<Decision> {
        unreachable!()
    }
}
fn task(id: &str, module: ObjectRef, input: Input, output: TaskOutput, function: &str) -> TaskSpec {
    TaskSpec {
        id: id.into(),
        module,
        input,
        output,
        function: function.into(),
        max_attempts: 1,
        notify: None,
    }
}
fn node(store: Arc<dyn Backend>, engine: Arc<dyn GuestEngine>, id: &str) -> Node {
    Node {
        store,
        engine,
        session: NodeSession {
            node: id.into(),
            incarnation: "boot".into(),
        },
        lease_ms: 5000,
    }
}
async fn result(store: &dyn Backend, id: &str) -> ObjectRef {
    let Some(TaskOutcome::Succeeded(output)) = store.task(id).await.unwrap().outcome else {
        panic!("{id} failed")
    };
    output
}

#[tokio::test]
async fn immutable_objects_roundtrip_and_instance_isolation() {
    let objects = MemoryObjectStore::default();
    let empty = objects.put(vec![]).await.unwrap();
    assert_eq!(
        empty.digest,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    let bytes = vec![0, 255, 128, 1];
    let r = objects.put(bytes.clone()).await.unwrap();
    assert_eq!(r, objects.clone().put(bytes.clone()).await.unwrap());
    assert_eq!(objects.clone().get(&r).await.unwrap(), bytes);
    assert!(matches!(
        MemoryObjectStore::default().get(&r).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn shuffle_payloads_use_only_the_injected_object_store() {
    let control = Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::ManualClock::default(),
    )));
    let objects = Arc::new(MemoryObjectStore::default());
    let store: Arc<dyn Backend> = Arc::new(Stores::new(control.clone(), objects.clone()));
    let module = store.put(b"mock-module".to_vec()).await.unwrap();
    let input = store
        .put(
            loom_shuffle::encode(&vec![ShuffleRecord {
                key: b"x".to_vec(),
                value: b"7".to_vec(),
            }])
            .unwrap(),
        )
        .await
        .unwrap();
    let layout = ShuffleLayout::hash(1);
    store
        .submit(task(
            "map",
            module.clone(),
            Input::Object(input.clone()),
            TaskOutput::Shuffle(layout.clone()),
            "map",
        ))
        .await
        .unwrap();
    store
        .submit(task(
            "reduce",
            module.clone(),
            Input::Shuffle {
                sources: vec![ShuffleSource::TaskResult("map".into())],
                layout,
                partition: 0,
            },
            TaskOutput::Value,
            "sum",
        ))
        .await
        .unwrap();
    let mapper = node(store.clone(), Arc::new(Sum), "mapper");
    mapper.register().await.unwrap();
    mapper.tick().await.unwrap();
    // A new composition shares the same control and object namespace, not local node state.
    let second: Arc<dyn Backend> = Arc::new(Stores::new(control.clone(), objects.clone()));
    let reducer = node(second.clone(), Arc::new(Sum), "reducer");
    reducer.register().await.unwrap();
    reducer.tick().await.unwrap();
    let output = result(second.as_ref(), "reduce").await;
    assert_eq!(objects.get(&output).await.unwrap(), b"7");
    let manifest = result(store.as_ref(), "map").await;
    let decoded: ShuffleManifest =
        loom_shuffle::decode(&objects.get(&manifest).await.unwrap()).unwrap();
    for reference in [
        &module,
        &input,
        &manifest,
        &decoded.partitions[0][0].object,
        &output,
    ] {
        assert!(
            matches!(control.get(reference).await, Err(Error::NotFound(_))),
            "payload leaked into coordination adapter"
        );
    }
    assert!(store.receipt(&task_job("reduce")).await.unwrap().is_some());
}

struct UnavailableObjects;
#[async_trait]
impl ObjectStore for UnavailableObjects {
    async fn put(&self, _: Bytes) -> Result<ObjectRef> {
        Err(Error::Unavailable("objects offline".into()))
    }
    async fn get(&self, _: &ObjectRef) -> Result<Bytes> {
        Err(Error::Unavailable("objects offline".into()))
    }
}
#[tokio::test]
async fn object_outage_does_not_fall_back_to_coordination_storage() {
    let control = Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::ManualClock::default(),
    )));
    let decoy = control
        .put(b"payload-in-wrong-store".to_vec())
        .await
        .unwrap();
    let store: Arc<dyn Backend> = Arc::new(Stores::new(control, Arc::new(UnavailableObjects)));
    assert!(matches!(
        store.get(&decoy).await,
        Err(Error::Unavailable(_))
    ));
    let worker = node(store.clone(), Arc::new(Sum), "worker");
    worker.register().await.unwrap();
    assert_eq!(store.members().await.unwrap().len(), 1);
    store
        .submit(task(
            "unavailable",
            ObjectRef::new("mock".into()),
            Input::Object(decoy),
            TaskOutput::Value,
            "map",
        ))
        .await
        .unwrap();
    worker.tick().await.unwrap();
    assert!(
        matches!(store.task("unavailable").await.unwrap().outcome, Some(TaskOutcome::Failed(error)) if error.contains("objects offline"))
    );
}

#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL and LOOM_TEST_COMPONENT"]
async fn postgres_coordination_with_independent_objects_and_real_wasm() {
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    let ns = format!("object-store-{}", uuid::Uuid::new_v4());
    let control = Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
    let objects = Arc::new(MemoryObjectStore::default());
    let store: Arc<dyn Backend> = Arc::new(Stores::new(control.clone(), objects.clone()));
    let module = store
        .put(std::fs::read(std::env::var("LOOM_TEST_COMPONENT").unwrap()).unwrap())
        .await
        .unwrap();
    let input = store
        .put(br#"{"message":"independent object storage"}"#.to_vec())
        .await
        .unwrap();
    store
        .submit(task(
            "first",
            module.clone(),
            Input::Object(input.clone()),
            TaskOutput::Value,
            "identity",
        ))
        .await
        .unwrap();
    store
        .submit(task(
            "second",
            module.clone(),
            Input::TaskResult("first".into()),
            TaskOutput::Value,
            "identity",
        ))
        .await
        .unwrap();
    // WasmEngine receives ONLY the object interface, proving module loading needs no ledger.
    let engine = Arc::new(loom_wasm_host::WasmEngine::new(objects.clone()).unwrap());
    let a = node(store.clone(), engine.clone(), "a");
    a.register().await.unwrap();
    a.tick().await.unwrap();
    let control2 = Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
    let reopened: Arc<dyn Backend> = Arc::new(Stores::new(control2.clone(), objects.clone()));
    let b = node(reopened.clone(), engine, "b");
    b.register().await.unwrap();
    b.tick().await.unwrap();
    let output = result(reopened.as_ref(), "second").await;
    assert_eq!(
        objects.get(&output).await.unwrap(),
        objects.get(&input).await.unwrap()
    );
    for reference in [&module, &input, &output] {
        assert!(matches!(
            control.get(reference).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            control2.get(reference).await,
            Err(Error::NotFound(_))
        ));
    }
    assert_eq!(reopened.task("first").await.unwrap().attempts, 1);
    assert_eq!(reopened.task("second").await.unwrap().attempts, 1);
}
