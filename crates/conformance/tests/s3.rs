use loom_contracts::*;
use loom_runtime::Node;
use std::sync::Arc;

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
#[ignore = "requires PostgreSQL, a compiled component, and S3 environment"]
async fn postgres_coordination_with_s3_and_real_wasm() {
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    let ns = format!("object-store-{}", uuid::Uuid::new_v4());
    let control = Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
    let config = loom_object_store_s3::S3Config::from_env(false).unwrap();
    let objects = Arc::new(config.connect(&ns).unwrap());
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
            Input::ObjectSlice(ObjectSlice {
                object: input.clone(),
                offset: 0,
                length: objects.size(&input).await.unwrap(),
            }),
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
    let objects2 = Arc::new(config.connect(&ns).unwrap());
    let reopened: Arc<dyn Backend> = Arc::new(Stores::new(control2.clone(), objects2.clone()));
    let engine2 = Arc::new(loom_wasm_host::WasmEngine::new(objects2).unwrap());
    let b = node(reopened.clone(), engine2, "b");
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
