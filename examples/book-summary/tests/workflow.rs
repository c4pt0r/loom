use async_trait::async_trait;
use loom_backend_memory::{ManualClock, SystemClock, memory};
use loom_book_summary::*;
use loom_contracts::*;
use loom_runtime::Node;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Fake {
    calls: Mutex<BTreeMap<String, usize>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl AgentExecutor for Fake {
    async fn run(&self, request: AgentRequest) -> Result<String> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = Active(&self.active);
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(15)).await;
        if let Some((_, schema)) = request.prompt.split_once("Use this schema: ") {
            let mut result: serde_json::Value = serde_json::from_str(schema).unwrap();
            let id = result["chapter_id"].as_str().unwrap().to_owned();
            let count = {
                let mut calls = self.calls.lock().unwrap();
                let n = calls.entry(id).or_default();
                *n += 1;
                *n
            };
            if request.source.contains("ALWAYS_FAIL")
                || (request.source.contains("FAIL_ONCE") && count == 1)
            {
                return Err(Error::Invalid("deliberate model failure".into()));
            }
            result["summary"] = request.source.into();
            if result["summary"].as_str().unwrap().contains("BAD_CITATION") {
                result["key_points"][0]["paragraph_end"] = 999.into();
            }
            Ok(result.to_string())
        } else {
            let summaries: Vec<ChapterSummary> = serde_json::from_str(&request.source).unwrap();
            *self
                .calls
                .lock()
                .unwrap()
                .entry("aggregate".into())
                .or_default() += 1;
            let (_, schema) = request
                .prompt
                .split_once("Return ONLY JSON, no markdown fences: ")
                .unwrap();
            let mut result: serde_json::Value = serde_json::from_str(schema).unwrap();
            result["themes"][0]["chapter_ids"] = serde_json::json!([summaries[0].chapter_id]);
            result["chapters"] = serde_json::json!(
                summaries
                    .iter()
                    .map(|c| Overview {
                        chapter_id: c.chapter_id.clone(),
                        summary: c.summary.clone()
                    })
                    .collect::<Vec<_>>()
            );
            Ok(result.to_string())
        }
    }
}
fn book(prefix: &str, count: usize) -> Book {
    Book {
        title: prefix.into(),
        chapters: (0..count)
            .map(|i| Chapter {
                id: format!("{prefix}-{i}"),
                title: format!("Chapter {i}"),
                source: Source::Inline(format!("Only {prefix} chapter {i}.")),
            })
            .collect(),
        concurrency: 2,
        max_attempts: 2,
        model: None,
    }
}
async fn node(store: Arc<dyn Backend>, fake: Arc<Fake>, name: &str) -> Node {
    let module = store.put(MODULE.to_vec()).await.unwrap();
    let objects = Arc::new(StoreObjects(store.clone()));
    Node {
        store,
        engine: Arc::new(BookEngine {
            objects,
            executor: fake,
            module,
        }),
        session: NodeSession {
            node: name.into(),
            incarnation: name.into(),
        },
        lease_ms: 5000,
    }
}
// Exercise only the public ObjectStore abstraction.
struct StoreObjects(Arc<dyn Backend>);
#[async_trait]
impl ObjectStore for StoreObjects {
    async fn put(&self, b: Bytes) -> Result<ObjectRef> {
        self.0.put(b).await
    }
    async fn get(&self, r: &ObjectRef) -> Result<Bytes> {
        self.0.get(r).await
    }
}
async fn state(store: &dyn Backend, id: &str) -> State {
    decode(&store.actor(id).await.unwrap().state).unwrap()
}

#[tokio::test]
async fn two_books_overlap_remain_isolated_and_preserve_order() {
    let store = Arc::new(memory(Arc::new(SystemClock)));
    let fake = Arc::new(Fake::default());
    let mut first = book("A", 5);
    first.chapters[1].source =
        Source::Object(store.put(b"Object source A".to_vec()).await.unwrap());
    let r = store
        .put(b"prefixSlice source Aend".to_vec())
        .await
        .unwrap();
    first.chapters[2].source = Source::ObjectSlice(ObjectSlice {
        object: r,
        offset: 6,
        length: 14,
    });
    first.chapters[3].source = Source::Inline("FAIL_ONCE".into());
    submit(store.as_ref(), "A", first.clone()).await.unwrap();
    submit(store.as_ref(), "A", first).await.unwrap(); // duplicate start
    submit(store.as_ref(), "B", book("B", 4)).await.unwrap();
    let node = node(store.clone(), fake.clone(), "worker").await;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let runner = tokio::spawn(async move {
        node.run_with_slots(4, async {
            let _ = stopped.await;
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let a = state(store.as_ref(), "A").await;
            let b = state(store.as_ref(), "B").await;
            assert!(a.active <= 2 && b.active <= 2);
            assert_ne!(a.phase, "failed");
            assert_ne!(b.phase, "failed");
            if a.phase == "completed" && b.phase == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    runner.await.unwrap().unwrap();
    assert!(fake.peak.load(Ordering::SeqCst) >= 2);
    assert!(fake.peak.load(Ordering::SeqCst) <= 4);
    for (id, count) in [("A", 5), ("B", 4)] {
        let s = state(store.as_ref(), id).await;
        let Artifact::Book(report) = decode(&store.get(&s.report.unwrap()).await.unwrap()).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            report
                .chapters
                .iter()
                .map(|c| c.chapter_id.clone())
                .collect::<Vec<_>>(),
            (0..count).map(|i| format!("{id}-{i}")).collect::<Vec<_>>()
        );
        for i in 0..count {
            assert_eq!(
                fake.calls.lock().unwrap()[&format!("{id}-{i}")],
                if id == "A" && i == 3 { 2 } else { 1 }
            );
        }
    }
    assert_eq!(fake.calls.lock().unwrap()["aggregate"], 2);
}

#[tokio::test]
async fn takeover_reuses_committed_chapter_and_fences_old_attempt() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(memory(clock.clone()));
    let fake = Arc::new(Fake::default());
    let mut b = book("recovery", 3);
    b.concurrency = 1;
    submit(store.as_ref(), "recovery", b).await.unwrap();
    let old = node(store.clone(), fake.clone(), "old").await;
    old.register().await.unwrap();
    for _ in 0..3 {
        assert!(old.tick().await.unwrap());
    } // start, chapter 0, completion => chapter 1
    let claim = store.claim(&old.session, 5000).await.unwrap().unwrap();
    let result = old.execute(&claim).await.unwrap(); // external result, deliberately not committed
    clock.advance(5001);
    let new = node(store.clone(), fake.clone(), "new").await;
    new.register().await.unwrap();
    assert!(store.commit(&claim, result).await.is_err());
    for _ in 0..16 {
        if state(store.as_ref(), "recovery").await.phase == "completed" {
            break;
        }
        assert!(new.tick().await.unwrap());
    }
    assert_eq!(state(store.as_ref(), "recovery").await.phase, "completed");
    let calls = fake.calls.lock().unwrap();
    assert_eq!(calls["recovery-0"], 1);
    assert_eq!(calls["recovery-1"], 2); // uncommitted model call can repeat
    assert_eq!(calls["recovery-2"], 1);
    assert_eq!(calls["aggregate"], 1);
}

#[tokio::test]
async fn exhausted_retries_or_invalid_citations_prevent_aggregation() {
    for source in ["ALWAYS_FAIL", "BAD_CITATION"] {
        let store = Arc::new(memory(Arc::new(SystemClock)));
        let fake = Arc::new(Fake::default());
        let mut b = book("bad", 1);
        b.chapters[0].source = Source::Inline(source.into());
        submit(store.as_ref(), "bad", b).await.unwrap();
        let n = node(store.clone(), fake.clone(), "worker").await;
        n.register().await.unwrap();
        for _ in 0..100 {
            if state(store.as_ref(), "bad").await.phase == "failed" {
                break;
            }
            n.tick().await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(state(store.as_ref(), "bad").await.phase, "failed");
        assert_eq!(fake.calls.lock().unwrap()["bad-0"], 2);
        assert!(!fake.calls.lock().unwrap().contains_key("aggregate"));
    }
}

#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn postgres_completed_book_survives_worker_restart_and_duplicate_submit() {
    let url = std::env::var("LOOM_TEST_DATABASE_URL").expect("LOOM_TEST_DATABASE_URL");
    let namespace = format!("book-test-{}", uuid::Uuid::new_v4());
    let first_store = Arc::new(
        loom_backend_postgres::connect(&url, &namespace)
            .await
            .unwrap(),
    );
    let fake = Arc::new(Fake::default());
    let b = book("persistent", 3);
    submit(first_store.as_ref(), "persistent", b.clone())
        .await
        .unwrap();
    let first = node(first_store.clone(), fake.clone(), "first").await;
    first.register().await.unwrap();
    for _ in 0..16 {
        if state(first_store.as_ref(), "persistent").await.phase == "completed" {
            break;
        }
        assert!(first.tick().await.unwrap());
    }
    let before = first_store.actor("persistent").await.unwrap();
    assert_eq!(decode::<State>(&before.state).unwrap().phase, "completed");
    first_store.drain(&first.session).await.unwrap();
    drop(first);
    drop(first_store);
    let second_store = Arc::new(
        loom_backend_postgres::connect(&url, &namespace)
            .await
            .unwrap(),
    );
    submit(second_store.as_ref(), "persistent", b.clone())
        .await
        .unwrap();
    let second = node(second_store.clone(), fake.clone(), "second").await;
    second.register().await.unwrap();
    assert!(!second.tick().await.unwrap());
    let after = second_store.actor("persistent").await.unwrap();
    assert_eq!(after.state, before.state);
    assert_eq!(after.revision, before.revision);
    let mut changed = b;
    changed.title = "different".into();
    assert!(
        submit(second_store.as_ref(), "persistent", changed)
            .await
            .is_err()
    );
    assert_eq!(fake.calls.lock().unwrap().values().sum::<usize>(), 4);
    second_store.drain(&second.session).await.unwrap();
}
