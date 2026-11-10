//! Deterministic overlap checks, not a throughput benchmark.
use async_trait::async_trait;
use loom_contracts::*;
use loom_runtime::Node;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

struct Engine {
    active: Mutex<BTreeSet<String>>,
    peak: AtomicUsize,
    overlap: AtomicBool,
    gate: Semaphore,
    hot_finished: AtomicUsize,
    hot_at_first_task: AtomicUsize,
}
impl Engine {
    fn new() -> Self {
        Self {
            active: Mutex::default(),
            peak: AtomicUsize::new(0),
            overlap: AtomicBool::new(false),
            gate: Semaphore::new(0),
            hot_finished: AtomicUsize::new(0),
            hot_at_first_task: AtomicUsize::new(usize::MAX),
        }
    }
    fn enter(&self, key: String) -> Result<Active<'_>> {
        let mut active = self.active.lock().unwrap();
        if !active.insert(key.clone()) {
            self.overlap.store(true, Ordering::SeqCst);
            return Err(Error::Invalid("actor reentered".into()));
        }
        self.peak.fetch_max(active.len(), Ordering::SeqCst);
        Ok(Active { engine: self, key })
    }
    async fn hold(&self) {
        self.gate.acquire().await.unwrap().forget();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
struct Active<'a> {
    engine: &'a Engine,
    key: String,
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.engine.active.lock().unwrap().remove(&self.key);
    }
}
#[async_trait]
impl GuestEngine for Engine {
    async fn task(&self, _: &ObjectRef, name: &str, input: Bytes) -> Result<Bytes> {
        let _active = self.enter(format!("task:{name}"))?;
        let _ = self.hot_at_first_task.compare_exchange(
            usize::MAX,
            self.hot_finished.load(Ordering::SeqCst),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.hold().await;
        Ok(input)
    }
    async fn actor(
        &self,
        _: &ObjectRef,
        name: &str,
        state: Bytes,
        message: Bytes,
    ) -> Result<Decision> {
        let _active = self.enter(format!("actor:{name}"))?;
        self.hold().await;
        let mut values: Vec<usize> = serde_json::from_slice(&state).unwrap();
        let value: usize = serde_json::from_slice(&message).unwrap();
        if value != values.len() {
            return Err(Error::Invalid("actor message order violated".into()));
        }
        values.push(value);
        if name == "hot" {
            self.hot_finished.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Decision {
            state: serde_json::to_vec(&values).unwrap(),
            reply: vec![],
            tasks: vec![],
            sends: vec![],
        })
    }
}
fn memory() -> Arc<dyn Backend> {
    Arc::new(loom_backend_memory::memory(Arc::new(
        loom_backend_memory::SystemClock,
    )))
}
fn node(store: Arc<dyn Backend>, engine: Arc<Engine>, id: &str, lease_ms: u64) -> Node {
    Node {
        store,
        engine,
        session: NodeSession {
            node: id.into(),
            incarnation: "boot".into(),
        },
        lease_ms,
    }
}
async fn actor(store: &dyn Backend, id: &str, messages: usize) {
    store
        .create_actor(ActorSpec {
            id: id.into(),
            module: ObjectRef::new("mock".into()),
            handler: id.into(),
            initial_state: b"[]".to_vec(),
            supervisor: Supervisor::default(),
        })
        .await
        .unwrap();
    for n in 0..messages {
        store
            .send(Envelope {
                id: format!("{id}-{n}"),
                actor: id.into(),
                payload: serde_json::to_vec(&n).unwrap(),
            })
            .await
            .unwrap();
    }
}
async fn wait_until(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition did not become true");
}
async fn mixed_load(store: Arc<dyn Backend>, nodes: usize, slots: usize) {
    const HOT: usize = 128;
    const COLD: usize = 16;
    const MESSAGES: usize = 8;
    const TASKS: usize = 32;
    actor(store.as_ref(), "hot", HOT).await;
    for n in 0..COLD {
        actor(store.as_ref(), &format!("cold-{n}"), MESSAGES).await;
    }
    for n in 0..TASKS {
        let id = format!("task-{n}");
        store
            .submit(TaskSpec {
                id: id.clone(),
                module: ObjectRef::new("mock".into()),
                function: id,
                input: Input::Inline(n.to_string().into_bytes()),
                output: TaskOutput::Value,
                max_attempts: 1,
                notify: None,
            })
            .await
            .unwrap();
    }
    let engine = Arc::new(Engine::new());
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut runners = Vec::new();
    for n in 0..nodes {
        let node = node(store.clone(), engine.clone(), &format!("node-{n}"), 5000);
        let mut stopped = stopped.clone();
        runners.push(tokio::spawn(async move {
            node.run_with_slots(slots, async {
                let _ = stopped.changed().await;
            })
            .await
        }));
    }
    wait_until(async || engine.active.lock().unwrap().len() == nodes * slots).await;
    assert_eq!(engine.peak.load(Ordering::SeqCst), nodes * slots);
    assert!(!engine.overlap.load(Ordering::SeqCst));
    // All first-wave actors are held simultaneously, proving actual overlap rather than fast sequential execution.
    engine.gate.add_permits(10_000);
    wait_until(async || {
        if store.actor("hot").await.unwrap().revision != HOT as u64 {
            return false;
        }
        for n in 0..COLD {
            if store.actor(&format!("cold-{n}")).await.unwrap().revision != MESSAGES as u64 {
                return false;
            }
        }
        for n in 0..TASKS {
            if store
                .task(&format!("task-{n}"))
                .await
                .unwrap()
                .outcome
                .is_none()
            {
                return false;
            }
        }
        true
    })
    .await;
    stop.send(true).unwrap();
    for runner in runners {
        tokio::time::timeout(Duration::from_secs(10), runner)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    for (id, count) in std::iter::once(("hot".to_string(), HOT))
        .chain((0..COLD).map(|n| (format!("cold-{n}"), MESSAGES)))
    {
        let state: Vec<usize> =
            serde_json::from_slice(&store.actor(&id).await.unwrap().state).unwrap();
        assert_eq!(state, (0..count).collect::<Vec<_>>());
        for n in 0..count {
            assert!(matches!(
                store
                    .receipt(&actor_job(&format!("{id}-{n}")))
                    .await
                    .unwrap(),
                Some(Receipt::Actor { .. })
            ));
        }
    }
    for n in 0..TASKS {
        let task = store.task(&format!("task-{n}")).await.unwrap();
        assert_eq!(task.attempts, 1);
        let Some(TaskOutcome::Succeeded(output)) = task.outcome else {
            panic!("task failed")
        };
        assert_eq!(
            store.get(&output).await.unwrap(),
            n.to_string().into_bytes()
        );
    }
    assert!(
        engine.hot_at_first_task.load(Ordering::SeqCst) < HOT,
        "hot mailbox starved ordinary tasks"
    );
    assert!(!engine.overlap.load(Ordering::SeqCst));
    assert_eq!(engine.peak.load(Ordering::SeqCst), nodes * slots);
    assert!(engine.active.lock().unwrap().is_empty());
}
#[tokio::test]
async fn multiple_actors_overlap_without_reentry_or_hot_mailbox_starvation() {
    mixed_load(memory(), 1, 4).await;
    mixed_load(memory(), 2, 4).await;
}
#[tokio::test]
async fn graceful_shutdown_drains_all_slots_and_keeps_renewing() {
    let store = memory();
    let engine = Arc::new(Engine::new());
    actor(store.as_ref(), "a", 1).await;
    actor(store.as_ref(), "b", 1).await;
    let node = node(store.clone(), engine.clone(), "drain", 300);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let runner = tokio::spawn(async move {
        node.run_with_slots(2, async {
            let _ = stopped.await;
        })
        .await
    });
    wait_until(async || engine.active.lock().unwrap().len() == 2).await;
    stop.send(()).unwrap();
    wait_until(async || store.members().await.unwrap().iter().any(|n| n.draining)).await;
    actor(store.as_ref(), "late", 1).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        store.members().await.unwrap().len(),
        1,
        "heartbeat stopped before slots drained"
    );
    engine.gate.add_permits(2);
    tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(store.actor("a").await.unwrap().revision, 1);
    assert_eq!(store.actor("b").await.unwrap().revision, 1);
    assert_eq!(store.actor("late").await.unwrap().revision, 0);
}
#[tokio::test]
async fn cancelled_runner_leaves_no_orphan_heartbeat_or_slot() {
    let store = memory();
    let engine = Arc::new(Engine::new());
    actor(store.as_ref(), "cancelled", 1).await;
    let node = node(store.clone(), engine.clone(), "cancel", 150);
    let runner = tokio::spawn(async move { node.run_with_slots(2, std::future::pending()).await });
    wait_until(async || engine.active.lock().unwrap().len() == 1).await;
    runner.abort();
    assert!(runner.await.unwrap_err().is_cancelled());
    wait_until(async || engine.active.lock().unwrap().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(store.members().await.unwrap().is_empty());
    assert_eq!(store.actor("cancelled").await.unwrap().revision, 0);
}
#[tokio::test]
async fn invalid_slot_counts_do_not_register_a_node() {
    let store = memory();
    let node = node(store.clone(), Arc::new(Engine::new()), "invalid", 1000);
    for slots in [0, 257] {
        assert!(
            node.run_with_slots(slots, std::future::pending())
                .await
                .is_err()
        );
    }
    assert!(store.members().await.unwrap().is_empty());
}
#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn postgres_multi_actor_overlap_ordering_and_mixed_load() {
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    for nodes in [1, 2] {
        let ns = format!("multi-actor-{}", uuid::Uuid::new_v4());
        let store = Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
        mixed_load(store, nodes, 4).await;
    }
}
