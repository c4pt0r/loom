use async_trait::async_trait;
use loom_backend_memory::{ManualClock, memory};
use loom_contracts::*;
use loom_runtime::Node;
use loom_shuffle::{decode, encode, partition_for};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn session(name: &str) -> NodeSession {
    NodeSession {
        node: name.into(),
        incarnation: "boot".into(),
    }
}
fn record(key: &[u8], value: u64) -> ShuffleRecord {
    ShuffleRecord {
        key: key.to_vec(),
        value: value.to_string().into_bytes(),
    }
}
fn map(id: &str, layout: &ShuffleLayout, records: &[ShuffleRecord]) -> TaskSpec {
    TaskSpec {
        id: id.into(),
        module: ObjectRef::new("mock".into()),
        function: "map".into(),
        input: Input::Inline(encode(&records).unwrap()),
        output: TaskOutput::Shuffle(layout.clone()),
        max_attempts: 3,
        notify: None,
    }
}
fn reducer(
    id: &str,
    layout: &ShuffleLayout,
    partition: u32,
    sources: Vec<ShuffleSource>,
) -> TaskSpec {
    TaskSpec {
        id: id.into(),
        module: ObjectRef::new("mock".into()),
        function: "sum".into(),
        input: Input::Shuffle {
            sources,
            layout: layout.clone(),
            partition,
        },
        output: TaskOutput::Value,
        max_attempts: 3,
        notify: None,
    }
}
struct Engine {
    calls: AtomicUsize,
    fail_call: usize,
}
#[async_trait]
impl GuestEngine for Engine {
    async fn task(&self, _: &ObjectRef, name: &str, input: Bytes) -> Result<Bytes> {
        if name == "map" {
            return Ok(input);
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_call {
            return Err(Error::Invalid("injected fold failure".into()));
        }
        let frame: ReduceFrame = decode(&input)?;
        let mut total: u64 = frame.state.as_deref().map(decode).transpose()?.unwrap_or(0);
        for record in frame.records {
            total += decode::<u64>(&record.value)?;
        }
        encode(&total)
    }
    async fn actor(&self, _: &ObjectRef, _: &str, _: Bytes, _: Bytes) -> Result<Decision> {
        unreachable!()
    }
}
fn node(store: Arc<dyn Backend>, engine: Arc<dyn GuestEngine>, name: &str) -> Node {
    Node {
        store,
        engine,
        session: session(name),
        lease_ms: 1000,
    }
}
fn engine() -> Arc<Engine> {
    Arc::new(Engine {
        calls: AtomicUsize::new(0),
        fail_call: 0,
    })
}
async fn output(store: &dyn Backend, id: &str) -> ObjectRef {
    let Some(TaskOutcome::Succeeded(r)) = store.task(id).await.unwrap().outcome else {
        panic!("task {id} incomplete")
    };
    r
}

#[test]
fn stable_hash_vectors_and_legacy_defaults() {
    let layout = ShuffleLayout { partitions: 7 };
    // Independent published SHA-256 prefixes for empty input and "abc".
    assert_eq!(
        partition_for(b"", &layout).unwrap(),
        0xe3b0c44298fc1c14_u64.wrapping_rem(7) as u32
    );
    assert_eq!(
        partition_for(b"abc", &layout).unwrap(),
        0xba7816bf8f01cfea_u64.wrapping_rem(7) as u32
    );
    let old = serde_json::json!({"id":"old","module":{"digest":"x"},"function":"echo",
        "input":{"Inline":[]},"max_attempts":1,"notify":null});
    let decoded: TaskSpec = serde_json::from_value(old).unwrap();
    assert_eq!(decoded.output, TaskOutput::Value);
}

#[tokio::test]
async fn opaque_records_route_deterministically_and_blocks_are_bounded() {
    let store = memory(Arc::new(ManualClock::default()));
    let layout = ShuffleLayout { partitions: 4 };
    let records = (0..1600)
        .map(|n| ShuffleRecord {
            key: vec![(n % 251) as u8, 0xff],
            value: [0, 255, 1, 127, 45, 9].repeat(16),
        })
        .chain([ShuffleRecord {
            key: vec![],
            value: vec![0xff],
        }])
        .collect::<Vec<_>>();
    let bytes = encode(&records).unwrap();
    let a = loom_shuffle::publish(&store, &layout, &bytes)
        .await
        .unwrap();
    let b = loom_shuffle::publish(&store, &layout, &bytes)
        .await
        .unwrap();
    assert_eq!(a, b);
    let manifest: ShuffleManifest = decode(&store.get(&a).await.unwrap()).unwrap();
    loom_shuffle::validate_manifest(&manifest, &layout).unwrap();
    let mut found = Vec::new();
    for (partition, blocks) in manifest.partitions.iter().enumerate() {
        for block in blocks {
            assert!(block.bytes as usize <= loom_shuffle::MAX_BLOCK_BYTES);
            found.extend(
                loom_shuffle::read_block(&store, block, &layout, partition as u32)
                    .await
                    .unwrap(),
            );
        }
    }
    let mut expected = records;
    expected.sort_by(|a, b| (&a.key, &a.value).cmp(&(&b.key, &b.value)));
    found.sort_by(|a, b| (&a.key, &a.value).cmp(&(&b.key, &b.value)));
    assert_eq!(found, expected);
    let oversized = vec![ShuffleRecord {
        key: vec![],
        value: vec![255; 70_000],
    }];
    assert!(
        loom_shuffle::publish(&store, &layout, &encode(&oversized).unwrap())
            .await
            .is_err()
    );
}

// Exercise the SAME dependency, fencing, and runtime path on memory and Postgres.
async fn barrier_and_takeover(store: Arc<dyn Backend>, clock: Option<Arc<ManualClock>>) {
    let layout = ShuffleLayout { partitions: 1 };
    let a = node(store.clone(), engine(), "old");
    let b = node(store.clone(), engine(), "survivor");
    store.register(a.session.clone(), 60_000).await.unwrap();
    store.register(b.session.clone(), 60_000).await.unwrap();
    store
        .submit(map("a", &layout, &[record(b"key", 7)]))
        .await
        .unwrap();
    store
        .submit(map("b", &layout, &[record(b"key", 7)]))
        .await
        .unwrap();
    store
        .submit(reducer(
            "reduce",
            &layout,
            0,
            vec![
                ShuffleSource::TaskResult("a".into()),
                ShuffleSource::TaskResult("b".into()),
            ],
        ))
        .await
        .unwrap();
    let stale = store.claim(&a.session, 300).await.unwrap().unwrap();
    let unpublished = a.execute(&stale).await.unwrap(); // all blocks written, no commit
    let second = store.claim(&b.session, 60_000).await.unwrap().unwrap();
    store
        .commit(&second, b.execute(&second).await.unwrap())
        .await
        .unwrap();
    // The uncommitted first map must prevent reduce from being claimed.
    assert!(store.claim(&b.session, 60_000).await.unwrap().is_none());
    match clock {
        Some(clock) => clock.advance(301),
        None => tokio::time::sleep(std::time::Duration::from_millis(350)).await,
    }
    let retry = store.claim(&b.session, 60_000).await.unwrap().unwrap();
    assert_eq!(retry.fence.job, task_job("a"));
    assert!(matches!(
        store.commit(&stale, unpublished).await,
        Err(Error::Fenced)
    ));
    let committed = b.execute(&retry).await.unwrap();
    let receipt = store.commit(&retry, committed.clone()).await.unwrap();
    assert_eq!(store.commit(&retry, committed).await.unwrap(), receipt);
    let r = store.claim(&b.session, 60_000).await.unwrap().unwrap();
    assert_eq!(r.fence.job, task_job("reduce"));
    store
        .commit(&r, b.execute(&r).await.unwrap())
        .await
        .unwrap();
    let value: u64 = decode(
        &store
            .get(&output(store.as_ref(), "reduce").await)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        value, 14,
        "identical manifests from DISTINCT maps must both contribute"
    );
    assert_eq!(store.task("a").await.unwrap().attempts, 2);
    assert_eq!(store.task("b").await.unwrap().attempts, 1);
}
#[tokio::test]
async fn memory_barrier_fencing_and_duplicate_manifests() {
    let clock = Arc::new(ManualClock::default());
    barrier_and_takeover(Arc::new(memory(clock.clone())), Some(clock)).await;
}
#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn postgres_shuffle_barrier_and_takeover() {
    let ns = format!("shuffle-{}", uuid::Uuid::new_v4());
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    let store = Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
    barrier_and_takeover(store, None).await;
    let reopened = loom_backend_postgres::connect(&url, &ns).await.unwrap();
    assert_eq!(
        decode::<u64>(
            &reopened
                .get(&output(&reopened, "reduce").await)
                .await
                .unwrap()
        )
        .unwrap(),
        14
    );
}

#[tokio::test]
async fn failed_dependency_does_not_wait_for_other_busy_maps() {
    let store: Arc<dyn Backend> = Arc::new(memory(Arc::new(ManualClock::default())));
    let n = node(store.clone(), engine(), "n");
    n.register().await.unwrap();
    let layout = ShuffleLayout { partitions: 2 };
    store.submit(map("busy", &layout, &[])).await.unwrap();
    let mut bad = map("bad", &layout, &[]);
    bad.max_attempts = 1;
    store.submit(bad).await.unwrap();
    store
        .submit(reducer(
            "downstream",
            &layout,
            0,
            vec![
                ShuffleSource::TaskResult("busy".into()),
                ShuffleSource::TaskResult("bad".into()),
            ],
        ))
        .await
        .unwrap();
    let _busy = store.claim(&n.session, 1000).await.unwrap().unwrap();
    let failed = store.claim(&n.session, 1000).await.unwrap().unwrap();
    store.fail(&failed, "bad mapper".into()).await.unwrap();
    assert!(store.claim(&n.session, 1000).await.unwrap().is_none());
    let r = store.task("downstream").await.unwrap();
    assert_eq!(r.attempts, 0);
    assert!(matches!(r.outcome, Some(TaskOutcome::Failed(e)) if e.contains("bad mapper")));
}

#[tokio::test]
async fn invalid_dependencies_are_rejected_before_enqueue() {
    let store = memory(Arc::new(ManualClock::default()));
    let layout = ShuffleLayout { partitions: 2 };
    store.submit(map("a", &layout, &[])).await.unwrap();
    let source = ShuffleSource::TaskResult("a".into());
    let cases = [
        reducer(
            "duplicate",
            &layout,
            0,
            vec![source.clone(), source.clone()],
        ),
        reducer(
            "missing",
            &layout,
            0,
            vec![ShuffleSource::TaskResult("missing".into())],
        ),
        reducer("range", &layout, 2, vec![source.clone()]),
        reducer(
            "mismatch",
            &ShuffleLayout { partitions: 3 },
            0,
            vec![source],
        ),
        map("zero", &ShuffleLayout { partitions: 0 }, &[]),
        reducer(
            "too-many",
            &layout,
            0,
            vec![ShuffleSource::Manifest(ObjectRef::new("x".into())); MAX_SHUFFLE_SOURCES + 1],
        ),
    ];
    for task in cases {
        let id = task.id.clone();
        assert!(store.submit(task).await.is_err());
        assert!(matches!(store.task(&id).await, Err(Error::NotFound(_))));
    }
}

#[tokio::test]
async fn reducer_reads_only_selected_partition_and_checks_routing() {
    let store: Arc<dyn Backend> = Arc::new(memory(Arc::new(ManualClock::default())));
    let layout = ShuffleLayout { partitions: 2 };
    let key = b"abc";
    let partition = partition_for(key, &layout).unwrap();
    let reference = loom_shuffle::publish(
        store.as_ref(),
        &layout,
        &encode(&vec![record(key, 11)]).unwrap(),
    )
    .await
    .unwrap();
    let mut manifest: ShuffleManifest = decode(&store.get(&reference).await.unwrap()).unwrap();
    let other = (1 - partition) as usize;
    manifest.partitions[other] = vec![ShuffleBlock {
        object: ObjectRef::new("not-present".into()),
        records: 1,
        bytes: 20,
    }];
    let reference = store.put(encode(&manifest).unwrap()).await.unwrap();
    let n = node(store.clone(), engine(), "n");
    n.register().await.unwrap();
    store
        .submit(reducer(
            "selected",
            &layout,
            partition,
            vec![ShuffleSource::Manifest(reference)],
        ))
        .await
        .unwrap();
    n.tick().await.unwrap();
    assert_eq!(
        decode::<u64>(
            &store
                .get(&output(store.as_ref(), "selected").await)
                .await
                .unwrap()
        )
        .unwrap(),
        11
    );
    // Move a real block to the wrong partition: the reducer rejects it.
    manifest.partitions[other] = manifest.partitions[partition as usize].clone();
    let bad = store.put(encode(&manifest).unwrap()).await.unwrap();
    store
        .submit(reducer(
            "bad",
            &layout,
            1 - partition,
            vec![ShuffleSource::Manifest(bad)],
        ))
        .await
        .unwrap();
    let claim = store.claim(&n.session, 1000).await.unwrap().unwrap();
    assert!(
        matches!(n.execute(&claim).await, Err(Error::Invalid(e)) if e.contains("wrong partition"))
    );
    manifest.version = 999;
    assert!(loom_shuffle::validate_manifest(&manifest, &layout).is_err());
}

#[tokio::test]
async fn reducer_retry_restarts_accumulator_and_empty_partition_initializes() {
    let clock = Arc::new(ManualClock::default());
    let store: Arc<dyn Backend> = Arc::new(memory(clock.clone()));
    let e = Arc::new(Engine {
        calls: AtomicUsize::new(0),
        fail_call: 2,
    });
    let n = node(store.clone(), e, "n");
    n.register().await.unwrap();
    let layout = ShuffleLayout { partitions: 1 };
    let reference = loom_shuffle::publish(
        store.as_ref(),
        &layout,
        &encode(&vec![record(b"same", 9)]).unwrap(),
    )
    .await
    .unwrap();
    store
        .submit(reducer(
            "retry",
            &layout,
            0,
            vec![
                ShuffleSource::Manifest(reference.clone()),
                ShuffleSource::Manifest(reference),
            ],
        ))
        .await
        .unwrap();
    n.tick().await.unwrap(); // fails after first accumulator update
    assert!(store.task("retry").await.unwrap().outcome.is_none());
    clock.advance(101);
    n.tick().await.unwrap();
    assert_eq!(
        decode::<u64>(
            &store
                .get(&output(store.as_ref(), "retry").await)
                .await
                .unwrap()
        )
        .unwrap(),
        18
    );
    assert_eq!(store.task("retry").await.unwrap().attempts, 2);
    store
        .submit(reducer("empty", &layout, 0, vec![]))
        .await
        .unwrap();
    n.tick().await.unwrap();
    assert_eq!(
        decode::<u64>(
            &store
                .get(&output(store.as_ref(), "empty").await)
                .await
                .unwrap()
        )
        .unwrap(),
        0
    );
}

struct FailingPublication {
    store: Arc<dyn Backend>,
    writes: AtomicUsize,
}
#[async_trait]
impl BlobStore for FailingPublication {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef> {
        if self.writes.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(Error::Unavailable(
                "injected second block write failure".into(),
            ));
        }
        self.store.put(bytes).await
    }
    async fn get(&self, object: &ObjectRef) -> Result<Bytes> {
        self.store.get(object).await
    }
}
#[tokio::test]
async fn interrupted_block_publication_never_returns_partial_manifest() {
    let store: Arc<dyn Backend> = Arc::new(memory(Arc::new(ManualClock::default())));
    let layout = ShuffleLayout { partitions: 1 };
    let records = vec![
        ShuffleRecord {
            key: b"opaque".to_vec(),
            value: vec![255; 10_000]
        };
        3
    ];
    let bytes = encode(&records).unwrap();
    let broken = FailingPublication {
        store: store.clone(),
        writes: AtomicUsize::new(0),
    };
    assert!(matches!(
        loom_shuffle::publish(&broken, &layout, &bytes).await,
        Err(Error::Unavailable(_))
    ));
    assert_eq!(broken.writes.load(Ordering::SeqCst), 2);
    // Retry may reuse orphaned immutable blocks, but exposes only a complete manifest.
    let result = loom_shuffle::publish(store.as_ref(), &layout, &bytes)
        .await
        .unwrap();
    let manifest: ShuffleManifest = decode(&store.get(&result).await.unwrap()).unwrap();
    assert_eq!(manifest.partitions[0].len(), 3);
    let mut recovered = Vec::new();
    for block in &manifest.partitions[0] {
        recovered.extend(
            loom_shuffle::read_block(store.as_ref(), block, &layout, 0)
                .await
                .unwrap(),
        );
    }
    assert_eq!(recovered, records);
}
