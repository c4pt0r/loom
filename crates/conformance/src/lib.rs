//! Black-box acceptance suite: uses only public contracts, not backend state-machine helpers.
#[cfg(test)]
mod tests {
    use loom_backend_memory::{ManualClock, memory};
    use loom_contracts::*;
    use std::{sync::Arc, time::Duration};

    fn owner(id: &str) -> NodeSession {
        NodeSession {
            node: id.into(),
            incarnation: format!("{id}-boot"),
        }
    }
    fn actor(id: &str) -> ActorSpec {
        ActorSpec {
            id: id.into(),
            module: ObjectRef::new("module".into()),
            handler: "counter".into(),
            initial_state: b"0".to_vec(),
            supervisor: Supervisor {
                max_attempts: 3,
                backoff_ms: 1,
            },
        }
    }
    fn task(id: &str) -> TaskSpec {
        TaskSpec {
            id: id.into(),
            module: ObjectRef::new("module".into()),
            function: "identity".into(),
            input: Input::Inline(b"1".to_vec()),
            output: loom_contracts::TaskOutput::Value,
            max_attempts: 2,
            notify: None,
        }
    }
    fn msg(id: &str, actor: &str) -> Envelope {
        Envelope {
            id: id.into(),
            actor: actor.into(),
            payload: b"1".to_vec(),
        }
    }
    fn change(revision: u64, value: &str) -> Completion {
        Completion::Actor {
            expected_revision: revision,
            decision: Decision {
                state: value.as_bytes().to_vec(),
                reply: b"ok".to_vec(),
                tasks: vec![],
                sends: vec![],
            },
        }
    }
    async fn advance(clock: &Option<Arc<ManualClock>>, ms: u64) {
        match clock {
            Some(c) => c.advance(ms),
            None => tokio::time::sleep(Duration::from_millis(ms + 30)).await,
        }
    }

    async fn atomicity(b: Arc<dyn Backend>) {
        let s = owner("atomic");
        b.register(s.clone(), 60_000).await.unwrap();
        b.create_actor(actor("atomic")).await.unwrap();
        b.send(msg("atomic-message", "atomic")).await.unwrap();
        b.send(msg("atomic-message", "atomic")).await.unwrap();
        let mut collision = msg("atomic-message", "atomic");
        collision.payload = b"different".to_vec();
        assert!(matches!(b.send(collision).await, Err(Error::Conflict(_))));
        let c = b.claim(&s, 10_000).await.unwrap().unwrap();
        let mut invalid = match change(0, "1") {
            Completion::Actor { decision, .. } => decision,
            _ => unreachable!(),
        };
        invalid.tasks.push(TaskIntent {
            function: "identity".into(),
            input: Input::Inline(b"1".to_vec()),
            output: loom_contracts::TaskOutput::Value,
            max_attempts: 1,
            notify_self: true,
        });
        invalid.sends.push(SendIntent {
            actor: "missing".into(),
            payload: vec![],
            delay_ms: 0,
        });
        assert!(
            b.commit(
                &c,
                Completion::Actor {
                    expected_revision: 0,
                    decision: invalid
                }
            )
            .await
            .is_err()
        );
        assert_eq!(b.actor("atomic").await.unwrap().revision, 0);
        assert!(b.receipt(&c.fence.job).await.unwrap().is_none());
        // Failed transaction must not leak its earlier task insertion.
        assert!(b.claim(&s, 1000).await.unwrap().is_none());
        let result = b.commit(&c, change(0, "1")).await.unwrap();
        assert_eq!(b.commit(&c, change(0, "1")).await.unwrap(), result);
        assert!(b.commit(&c, change(0, "2")).await.is_err());
        assert_eq!(b.actor("atomic").await.unwrap().state, b"1");
        assert!(b.claim(&s, 1000).await.unwrap().is_none());
    }

    async fn fencing(b: Arc<dyn Backend>, clock: Option<Arc<ManualClock>>) {
        let a = owner("fence-a");
        let z = owner("fence-b");
        b.register(a.clone(), 60_000).await.unwrap();
        b.register(z.clone(), 60_000).await.unwrap();
        b.create_actor(actor("fenced")).await.unwrap();
        b.send(msg("fenced-message", "fenced")).await.unwrap();
        let old = b.claim(&a, 100).await.unwrap().unwrap();
        assert!(b.claim(&z, 100).await.unwrap().is_none());
        advance(&clock, 150).await;
        let new = b.claim(&z, 10_000).await.unwrap().unwrap();
        assert!(new.fence.epoch > old.fence.epoch);
        assert!(matches!(
            b.commit(&old, change(0, "wrong")).await,
            Err(Error::Fenced)
        ));
        b.commit(&new, change(0, "right")).await.unwrap();
        assert_eq!(b.actor("fenced").await.unwrap().state, b"right");
    }

    async fn actor_task_chain(b: Arc<dyn Backend>) {
        let s = owner("chain");
        b.register(s.clone(), 60_000).await.unwrap();
        b.create_actor(actor("chain")).await.unwrap();
        b.send(msg("chain-start", "chain")).await.unwrap();
        let c = b.claim(&s, 10_000).await.unwrap().unwrap();
        let decision = Decision {
            state: b"waiting".to_vec(),
            reply: vec![],
            tasks: vec![TaskIntent {
                function: "identity".into(),
                input: Input::Inline(b"result".to_vec()),
                output: loom_contracts::TaskOutput::Value,
                max_attempts: 2,
                notify_self: true,
            }],
            sends: vec![],
        };
        let completion = Completion::Actor {
            expected_revision: 0,
            decision,
        };
        b.commit(&c, completion.clone()).await.unwrap();
        b.commit(&c, completion).await.unwrap();
        let t = b.claim(&s, 10_000).await.unwrap().unwrap();
        assert!(matches!(t.work, Work::Task { .. }));
        let output = b.put(b"result".to_vec()).await.unwrap();
        let complete = Completion::Task {
            output: output.clone(),
        };
        b.commit(&t, complete.clone()).await.unwrap();
        b.commit(&t, complete).await.unwrap();
        let notification = b.claim(&s, 10_000).await.unwrap().unwrap();
        let Work::Actor { message, .. } = &notification.work else {
            panic!("expected completion message")
        };
        let done: TaskCompleted = serde_json::from_slice(&message.payload).unwrap();
        assert_eq!(done.outcome, TaskOutcome::Succeeded(output));
        b.commit(&notification, change(1, "done")).await.unwrap();
        assert!(b.claim(&s, 10_000).await.unwrap().is_none());
        assert_eq!(b.actor("chain").await.unwrap().revision, 2);
    }

    async fn ordering_and_timer(b: Arc<dyn Backend>, clock: Option<Arc<ManualClock>>) {
        let s = owner("timer");
        b.register(s.clone(), 60_000).await.unwrap();
        b.create_actor(actor("timer")).await.unwrap();
        b.send(msg("timer-start", "timer")).await.unwrap();
        let c = b.claim(&s, 10_000).await.unwrap().unwrap();
        let decision = Decision {
            state: b"waiting".to_vec(),
            reply: vec![],
            tasks: vec![],
            sends: vec![SendIntent {
                actor: "timer".into(),
                payload: b"timer-fired".to_vec(),
                delay_ms: 1000,
            }],
        };
        b.commit(
            &c,
            Completion::Actor {
                expected_revision: 0,
                decision,
            },
        )
        .await
        .unwrap();
        assert!(b.claim(&s, 10_000).await.unwrap().is_none());
        b.send(msg("ordinary", "timer")).await.unwrap();
        let ordinary = b.claim(&s, 10_000).await.unwrap().unwrap();
        advance(&clock, 1100).await;
        // Earlier-sequenced timer becomes ready while a later message is executing.
        assert!(b.claim(&s, 10_000).await.unwrap().is_none());
        b.commit(&ordinary, change(1, "ordinary")).await.unwrap();
        let timer = b.claim(&s, 10_000).await.unwrap().unwrap();
        let Work::Actor { message, .. } = &timer.work else {
            panic!()
        };
        assert_eq!(message.payload, b"timer-fired");
        b.commit(&timer, change(2, "timer")).await.unwrap();
    }

    async fn membership(b: Arc<dyn Backend>, clock: Option<Arc<ManualClock>>) {
        let old = owner("member");
        b.register(old.clone(), 100).await.unwrap();
        let new = NodeSession {
            node: old.node.clone(),
            incarnation: "new-boot".into(),
        };
        assert!(matches!(
            b.register(new.clone(), 100).await,
            Err(Error::Conflict(_))
        ));
        advance(&clock, 150).await;
        assert!(matches!(b.renew(&old, 1000).await, Err(Error::Fenced)));
        assert!(matches!(
            b.register(old.clone(), 1000).await,
            Err(Error::Fenced)
        ));
        b.register(new.clone(), 60_000).await.unwrap();
        assert!(matches!(b.drain(&old).await, Err(Error::Fenced)));
        b.submit(task("draining-task")).await.unwrap();
        b.drain(&new).await.unwrap();
        assert!(b.claim(&new, 1000).await.unwrap().is_none());
        let worker = owner("member-worker");
        b.register(worker.clone(), 60_000).await.unwrap();
        let c = b.claim(&worker, 1000).await.unwrap().unwrap();
        let output = b.put(vec![]).await.unwrap();
        b.commit(&c, Completion::Task { output }).await.unwrap();
    }

    async fn task_dependency(b: Arc<dyn Backend>) {
        let s = owner("dependency");
        b.register(s.clone(), 60_000).await.unwrap();
        b.submit(task("parent")).await.unwrap();
        let mut child = task("child");
        child.input = Input::TaskResult("parent".into());
        b.submit(child).await.unwrap();
        let parent = b.claim(&s, 1000).await.unwrap().unwrap();
        assert!(b.claim(&s, 1000).await.unwrap().is_none());
        let output = b.put(b"dependency-result".to_vec()).await.unwrap();
        assert_eq!(b.put(b"dependency-result".to_vec()).await.unwrap(), output);
        b.commit(
            &parent,
            Completion::Task {
                output: output.clone(),
            },
        )
        .await
        .unwrap();
        let child = b.claim(&s, 1000).await.unwrap().unwrap();
        let Work::Task { input, .. } = &child.work else {
            panic!()
        };
        assert_eq!(*input, Input::Object(output.clone()));
        assert_eq!(b.get(&output).await.unwrap(), b"dependency-result");
        b.commit(&child, Completion::Task { output }).await.unwrap();
    }

    async fn supervisor(b: Arc<dyn Backend>, clock: Option<Arc<ManualClock>>) {
        let s = owner("supervisor");
        b.register(s.clone(), 60_000).await.unwrap();
        b.create_actor(actor("supervised")).await.unwrap();
        b.send(msg("poison", "supervised")).await.unwrap();
        b.send(msg("next", "supervised")).await.unwrap();
        for attempt in 1..=3 {
            let c = b.claim(&s, 10_000).await.unwrap().unwrap();
            assert_eq!(c.fence.job, actor_job("poison"));
            assert_eq!(c.attempt, attempt);
            b.fail(&c, "guest trap".into()).await.unwrap();
            b.fail(&c, "guest trap".into()).await.unwrap();
            advance(&clock, 10).await;
        }
        assert!(matches!(
            b.receipt(&actor_job("poison")).await.unwrap(),
            Some(Receipt::Failed(_))
        ));
        assert_eq!(b.actor("supervised").await.unwrap().revision, 0);
        let next = b.claim(&s, 10_000).await.unwrap().unwrap();
        assert_eq!(next.fence.job, actor_job("next"));
        b.commit(&next, change(0, "healthy")).await.unwrap();
    }

    fn fixture() -> (Arc<dyn Backend>, Option<Arc<ManualClock>>) {
        let clock = Arc::new(ManualClock::default());
        (Arc::new(memory(clock.clone())), Some(clock))
    }
    #[tokio::test]
    async fn memory_atomic_commit_and_idempotency() {
        atomicity(fixture().0).await;
    }
    #[tokio::test]
    async fn memory_stale_owner_fenced() {
        let (b, c) = fixture();
        fencing(b, c).await;
    }
    #[tokio::test]
    async fn memory_actor_task_notification_atomicity() {
        actor_task_chain(fixture().0).await;
    }
    #[tokio::test]
    async fn memory_actor_serialization_and_timers() {
        let (b, c) = fixture();
        ordering_and_timer(b, c).await;
    }
    #[tokio::test]
    async fn memory_membership_incarnation_and_drain() {
        let (b, c) = fixture();
        membership(b, c).await;
    }
    #[tokio::test]
    async fn memory_object_refs_and_dependencies() {
        task_dependency(fixture().0).await;
    }
    #[tokio::test]
    async fn memory_supervisor_restart_budget() {
        let (b, c) = fixture();
        supervisor(b, c).await;
    }

    #[tokio::test]
    async fn memory_concurrent_claims_have_one_winner() {
        let b = fixture().0;
        b.submit(task("contended")).await.unwrap();
        let mut joins = vec![];
        for i in 0..16 {
            let s = owner(&format!("concurrent-{i}"));
            b.register(s.clone(), 60_000).await.unwrap();
            let b = b.clone();
            joins.push(tokio::spawn(async move {
                b.claim(&s, 1000).await.unwrap().is_some()
            }));
        }
        let mut winners = 0;
        for j in joins {
            if j.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
    }

    #[tokio::test]
    #[ignore = "requires LOOM_TEST_DATABASE_URL; uses a fresh isolated namespace"]
    async fn postgres_contract_and_reconnect() {
        let url = std::env::var("LOOM_TEST_DATABASE_URL").expect("LOOM_TEST_DATABASE_URL");
        let ns = format!("conformance-{}", uuid::Uuid::new_v4());
        let b: Arc<dyn Backend> =
            Arc::new(loom_backend_postgres::connect(&url, &ns).await.unwrap());
        atomicity(b.clone()).await;
        fencing(b.clone(), None).await;
        actor_task_chain(b.clone()).await;
        ordering_and_timer(b.clone(), None).await;
        membership(b.clone(), None).await;
        task_dependency(b.clone()).await;
        supervisor(b.clone(), None).await;
        drop(b);
        let reopened = loom_backend_postgres::connect(&url, &ns).await.unwrap();
        assert_eq!(reopened.actor("chain").await.unwrap().state, b"done");
        assert_eq!(reopened.actor("chain").await.unwrap().revision, 2);
        // Different physical connections, competing for the same work.
        reopened.submit(task("pg-contended")).await.unwrap();
        let mut joins = vec![];
        for i in 0..8 {
            let connection = loom_backend_postgres::connect(&url, &ns).await.unwrap();
            let s = owner(&format!("pg-concurrent-{i}"));
            connection.register(s.clone(), 60_000).await.unwrap();
            joins.push(tokio::spawn(async move {
                connection.claim(&s, 10_000).await.unwrap().is_some()
            }));
        }
        let mut winners = 0;
        for j in joins {
            if j.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
    }

    #[tokio::test]
    #[ignore = "requires LOOM_TEST_COMPONENT built from the Rust example"]
    async fn wasm_sandbox_and_actor_restart() {
        let path = std::env::var("LOOM_TEST_COMPONENT").expect("LOOM_TEST_COMPONENT");
        let path = std::path::PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(path)
        };
        let b = fixture().0;
        let module = b.put(std::fs::read(path).unwrap()).await.unwrap();
        let engine = loom_wasm_host::WasmEngine::new(b.clone()).unwrap();
        assert_eq!(
            engine
                .task(&module, "identity", b"42".to_vec())
                .await
                .unwrap(),
            b"42"
        );
        assert!(
            engine
                .task(&module, "spin", vec![])
                .await
                .unwrap_err()
                .to_string()
                .contains("fuel")
        );
        assert!(engine.task(&module, "fail", vec![]).await.is_err());
        // Fresh store/instance after a trap must remain usable.
        assert_eq!(
            engine
                .task(&module, "identity", b"ok".to_vec())
                .await
                .unwrap(),
            b"ok"
        );
        let decision = engine
            .actor(
                &module,
                "research",
                b"{}".to_vec(),
                br#"{"start":["a b"]}"#.to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(decision.tasks.len(), 1);
        assert!(
            engine
                .actor(
                    &module,
                    "research",
                    decision.state,
                    br#"{"approve":true}"#.to_vec()
                )
                .await
                .is_err()
        );
    }
}
