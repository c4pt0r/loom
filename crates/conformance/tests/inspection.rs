use loom_contracts::*;
use std::{collections::BTreeMap, sync::Arc};

async fn fixture(store: &dyn Backend) {
    let module = store.put(b"inspection fixture".to_vec()).await.unwrap();
    let owner = NodeSession {
        node: "worker".into(),
        incarnation: "current".into(),
    };
    store.register(owner.clone(), 60000).await.unwrap();
    store
        .create_actor(ActorSpec {
            id: "actor".into(),
            module: module.clone(),
            handler: "coordinator".into(),
            initial_state: b"{\"phase\":\"summarizing\"}".to_vec(),
            supervisor: Supervisor::default(),
        })
        .await
        .unwrap();
    store
        .send(Envelope {
            id: "turn-one".into(),
            actor: "actor".into(),
            payload: b"{}".to_vec(),
        })
        .await
        .unwrap();
    let claim = store.claim(&owner, 60000).await.unwrap().unwrap();
    assert_eq!(claim.fence.job, "message:turn-one");
    store
        .send(Envelope {
            id: "turn-two".into(),
            actor: "actor".into(),
            payload: b"{}".to_vec(),
        })
        .await
        .unwrap();
    for (id, attempts) in [
        ("running", 2),
        ("success", 1),
        ("failed", 1),
        ("retrying", 3),
    ] {
        store
            .submit(TaskSpec {
                id: id.into(),
                module: module.clone(),
                function: format!("fn-{id}"),
                input: Input::Inline(b"{\"value\":1}".to_vec()),
                output: TaskOutput::Value,
                max_attempts: attempts,
                notify: None,
            })
            .await
            .unwrap();
        let c = store.claim(&owner, 60000).await.unwrap().unwrap();
        assert_eq!(c.fence.job, task_job(id));
        if id == "success" {
            let output = store.put(b"{\"answer\":42}".to_vec()).await.unwrap();
            store.commit(&c, Completion::Task { output }).await.unwrap();
        }
        if id == "failed" || id == "retrying" {
            store
                .fail(&c, "model request timed out".into())
                .await
                .unwrap();
        }
    }
    for (id, input) in [
        ("parent", Input::Inline(b"null".to_vec())),
        ("child", Input::TaskResult("parent".into())),
    ] {
        store
            .submit(TaskSpec {
                id: id.into(),
                module: module.clone(),
                function: "map-reduce".into(),
                input,
                output: TaskOutput::Value,
                max_attempts: 1,
                notify: None,
            })
            .await
            .unwrap();
    }
}
async fn assertions(inspector: &dyn InspectionStore, store: &dyn Backend) {
    let page = inspector
        .inspect(InspectQuery {
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    let statuses: BTreeMap<_, _> = page.jobs.iter().map(|j| (j.id.as_str(), j.phase)).collect();
    assert_eq!(
        statuses,
        BTreeMap::from([
            ("task:child", JobPhase::Waiting),
            ("task:parent", JobPhase::Queued),
            ("task:failed", JobPhase::Failed),
            ("task:retrying", JobPhase::Retrying),
            ("task:running", JobPhase::Running),
            ("task:success", JobPhase::Succeeded)
        ])
    );
    assert_eq!(page.total, 6);
    assert_eq!(page.summary.nodes["online"], 1);
    assert_eq!(page.summary.actors, 1);
    assert_eq!(page.summary.tasks.values().sum::<u64>(), 6);
    let retry = inspector.inspect_job("task:retrying").await.unwrap();
    assert_eq!(retry.job.error.as_deref(), Some("model request timed out"));
    assert_eq!(retry.job.attempts, 1);
    assert_eq!(retry.job.max_attempts, 3);
    let success = inspector.inspect_job("task:success").await.unwrap();
    assert_eq!(
        store.get(&success.job.output.unwrap()).await.unwrap(),
        b"{\"answer\":42}"
    );
    let a = inspector.inspect_actor("actor").await.unwrap();
    assert_eq!(a.pending, 1);
    assert_eq!(a.running, 1);
    assert!(a.state.text.contains("summarizing"));
    let messages = inspector
        .inspect(InspectQuery {
            resource: InspectResource::Messages,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(messages.total, 2);
    assert_eq!(messages.jobs[0].phase, JobPhase::Waiting);
    assert_eq!(messages.jobs[1].phase, JobPhase::Running);
    let nodes = inspector
        .inspect(InspectQuery {
            resource: InspectResource::Nodes,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(nodes.nodes[0].running, 2);
    let filtered = inspector
        .inspect(InspectQuery {
            search: "FN-FAILED".into(),
            phase: Some(JobPhase::Failed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(filtered.total, 1);
    assert_eq!(filtered.jobs[0].id, "task:failed");
    let none = inspector
        .inspect(InspectQuery {
            search: "%_' OR 1=1".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(none.total, 0);
    let first = inspector
        .inspect(InspectQuery {
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    let second = inspector
        .inspect(InspectQuery {
            limit: 2,
            offset: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first.total, 6);
    assert!(first.jobs[1].seq > second.jobs[0].seq);
    assert!(
        inspector
            .inspect(InspectQuery {
                limit: 101,
                ..Default::default()
            })
            .await
            .is_err()
    );
    assert!(inspector.inspect_job("not-present").await.is_err());
    assert!(inspector.inspect_actor("not-present").await.is_err());
    // Reads must not advance attempts or commit any jobs.
    assert_eq!(store.task("retrying").await.unwrap().attempts, 1);
    assert!(store.task("parent").await.unwrap().outcome.is_none());
}
#[tokio::test]
async fn memory_inspection_statuses_pagination_and_details() {
    let s = loom_backend_memory::memory(Arc::new(loom_backend_memory::SystemClock));
    fixture(&s).await;
    assertions(&s, &s).await;
}
#[tokio::test]
async fn expired_nodes_and_replaced_incarnations_are_not_running() {
    let clock = Arc::new(loom_backend_memory::ManualClock::default());
    let s = loom_backend_memory::memory(clock.clone());
    fixture(&s).await;
    clock.advance(60001);
    let p = s.inspect(InspectQuery::default()).await.unwrap();
    assert_eq!(p.summary.nodes["expired"], 1);
    assert!(!p.summary.tasks.contains_key("running"));
    assert_eq!(
        s.inspect_job("task:running").await.unwrap().job.phase,
        JobPhase::Retrying
    );
    s.register(
        NodeSession {
            node: "worker".into(),
            incarnation: "replacement".into(),
        },
        60000,
    )
    .await
    .unwrap();
    let p = s
        .inspect(InspectQuery {
            resource: InspectResource::Nodes,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(p.nodes[0].status, "online");
    assert_eq!(p.nodes[0].running, 0);
}
#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn postgres_inspection_statuses_and_namespace_isolation() {
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    let ns = format!("inspect-{}", uuid::Uuid::new_v4());
    let s = loom_backend_postgres::connect(&url, &ns).await.unwrap();
    fixture(&s).await;
    assertions(&s, &s).await;
    let other = loom_backend_postgres::connect(&url, &format!("{ns}-other"))
        .await
        .unwrap();
    let p = other.inspect(InspectQuery::default()).await.unwrap();
    assert_eq!(p.total, 0);
    assert!(p.summary.nodes.is_empty());
    assert!(other.inspect_job("task:success").await.is_err());
    // Simulate lease loss and same-node replacement in persisted rows.
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.execute("UPDATE loom_nodes SET lease_until=0, body=jsonb_set(body::jsonb,'{lease_until}','0')::text WHERE namespace=$1",&[&ns]).await.unwrap();
    assert_eq!(
        s.inspect_job("task:running").await.unwrap().job.phase,
        JobPhase::Retrying
    );
    s.register(
        NodeSession {
            node: "worker".into(),
            incarnation: "replacement".into(),
        },
        60000,
    )
    .await
    .unwrap();
    let p = s
        .inspect(InspectQuery {
            resource: InspectResource::Nodes,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(p.nodes[0].running, 0);
    assert_eq!(
        s.inspect_job("task:running").await.unwrap().job.phase,
        JobPhase::Retrying
    );
}
