//! SQL locks are deliberate test fixtures; behavior is checked through public contracts.
use loom_contracts::*;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio_postgres::NoTls;

fn task(id: &str) -> TaskSpec {
    TaskSpec {
        id: id.into(),
        module: ObjectRef::new("mock".into()),
        function: "identity".into(),
        input: Input::Inline(vec![]),
        output: TaskOutput::Value,
        max_attempts: 3,
        notify: None,
    }
}
fn owner(id: &str) -> NodeSession {
    NodeSession {
        node: id.into(),
        incarnation: "boot".into(),
    }
}
async fn sql() -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&std::env::var("LOOM_TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}
#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn locked_rows_do_not_block_other_work_and_concurrent_claims_are_unique() {
    let ns = format!("rows-{}", uuid::Uuid::new_v4());
    let store = Arc::new(
        loom_backend_postgres::connect(&std::env::var("LOOM_TEST_DATABASE_URL").unwrap(), &ns)
            .await
            .unwrap(),
    );
    let session = owner("initial");
    store.register(session.clone(), 60_000).await.unwrap();
    store.submit(task("first")).await.unwrap();
    store.submit(task("second")).await.unwrap();
    let mut client = sql().await;
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM loom_jobs WHERE namespace=$1 AND id='task:first' FOR UPDATE",
        &[&ns],
    )
    .await
    .unwrap();
    // Also hold the legacy namespace row: normal operations must no longer touch it.
    tx.query_one(
        "SELECT namespace FROM loom_ledgers WHERE namespace=$1 FOR UPDATE",
        &[&ns],
    )
    .await
    .unwrap();
    let claim = tokio::time::timeout(Duration::from_secs(2), store.claim(&session, 60_000))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(claim.fence.job, "task:second");
    tokio::time::timeout(Duration::from_secs(2), store.task("first"))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), store.submit(task("third")))
        .await
        .unwrap()
        .unwrap();
    store
        .commit(
            &claim,
            Completion::Task {
                output: ObjectRef::new("second-result".into()),
            },
        )
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    for n in 0..48 {
        store.submit(task(&format!("parallel-{n}"))).await.unwrap();
    }
    let mut handles = Vec::new();
    for n in 0..8 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let session = owner(&format!("worker-{n}"));
            store.register(session.clone(), 60_000).await.unwrap();
            let mut claimed = Vec::new();
            while let Some(claim) = store.claim(&session, 60_000).await.unwrap() {
                claimed.push(claim.fence.job.clone());
                let completion = Completion::Task {
                    output: ObjectRef::new(claim.fence.job.clone()),
                };
                let receipt = store.commit(&claim, completion.clone()).await.unwrap();
                assert_eq!(receipt, store.commit(&claim, completion).await.unwrap());
            }
            claimed
        }));
    }
    let mut ids = BTreeSet::new();
    let mut count = 0;
    for handle in handles {
        for id in handle.await.unwrap() {
            count += 1;
            assert!(ids.insert(id));
        }
    }
    assert_eq!(count, 50); // 48 new jobs plus first and third.
    for n in 0..48 {
        assert_eq!(
            store.task(&format!("parallel-{n}")).await.unwrap().attempts,
            1
        );
    }
    let object = store.put(b"0123456789".to_vec()).await.unwrap();
    assert_eq!(store.get_range(&object, 2, 3).await.unwrap(), b"234");
    assert!(store.get_range(&object, 9, 2).await.is_err());
    assert_eq!(
        store.get_range(&object, 10, 0).await.unwrap(),
        Vec::<u8>::new()
    );
    assert_eq!(store.get(&object).await.unwrap(), b"0123456789");
}

#[tokio::test]
#[ignore = "requires LOOM_TEST_DATABASE_URL"]
async fn legacy_ledger_migration_preserves_state_receipts_and_pending_work() {
    use loom_backend_core::{Answer, Ledger, Op};
    let ns = format!("migration-{}", uuid::Uuid::new_v4());
    let url = std::env::var("LOOM_TEST_DATABASE_URL").unwrap();
    // Ensure schema exists before inserting an isolated legacy fixture.
    let _ = loom_backend_postgres::connect(&url, &format!("{ns}-bootstrap"))
        .await
        .unwrap();
    let mut ledger = Ledger::default();
    let session = owner("legacy");
    ledger
        .apply(Op::Register(session.clone(), 60_000), 1)
        .unwrap();
    ledger.apply(Op::Submit(task("completed")), 1).unwrap();
    let Answer::Claim(Some(claim)) = ledger.apply(Op::Claim(session, 1000), 2).unwrap() else {
        panic!()
    };
    let completed = Completion::Task {
        output: ObjectRef::new("legacy-output".into()),
    };
    ledger
        .apply(Op::Commit(claim.clone(), completed.clone()), 3)
        .unwrap();
    let mut dependent = task("pending");
    dependent.input = Input::TaskResult("completed".into());
    ledger.apply(Op::Submit(dependent), 4).unwrap();
    ledger
        .apply(
            Op::Create(ActorSpec {
                id: "actor".into(),
                module: ObjectRef::new("module".into()),
                handler: "counter".into(),
                initial_state: b"7".to_vec(),
                supervisor: Supervisor::default(),
            }),
            5,
        )
        .unwrap();
    let client = sql().await;
    client
        .execute(
            "INSERT INTO loom_ledgers(namespace,schema_version,body) VALUES($1,1,$2)",
            &[&ns, &serde_json::to_string(&ledger).unwrap()],
        )
        .await
        .unwrap();
    let store = loom_backend_postgres::connect(&url, &ns).await.unwrap();
    assert_eq!(store.actor("actor").await.unwrap().state, b"7");
    assert_eq!(store.task("completed").await.unwrap().attempts, 1);
    assert!(matches!(
        store.commit(&claim, completed).await.unwrap(),
        Receipt::Task(TaskOutcome::Succeeded(_))
    ));
    let fresh = owner("new");
    store.register(fresh.clone(), 60_000).await.unwrap();
    let pending = store.claim(&fresh, 60_000).await.unwrap().unwrap();
    assert_eq!(pending.fence.job, "task:pending");
    let Work::Task { input, .. } = &pending.work else {
        panic!()
    };
    assert_eq!(
        *input,
        Input::Object(ObjectRef::new("legacy-output".into()))
    );
    store
        .commit(
            &pending,
            Completion::Task {
                output: ObjectRef::new("done".into()),
            },
        )
        .await
        .unwrap();
    let reopened = loom_backend_postgres::connect(&url, &ns).await.unwrap();
    assert!(reopened.task("pending").await.unwrap().outcome.is_some());
    let version: i32 = client
        .query_one(
            "SELECT schema_version FROM loom_ledgers WHERE namespace=$1",
            &[&ns],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(version, 2);
}
