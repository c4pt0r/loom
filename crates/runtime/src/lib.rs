//! Execution orchestration and shuffle folds. No storage adapter or Wasm engine dependency.
use loom_contracts::*;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct Node {
    pub store: Arc<dyn Backend>,
    pub engine: Arc<dyn GuestEngine>,
    pub session: NodeSession,
    pub lease_ms: Millis,
}
impl Node {
    pub async fn register(&self) -> Result<()> {
        self.store
            .register(self.session.clone(), self.lease_ms)
            .await
    }
    pub async fn execute(&self, claim: &Claim) -> Result<Completion> {
        match &claim.work {
            Work::Task { spec, input } => {
                let bytes = match input {
                    Input::Shuffle {
                        sources,
                        layout,
                        partition,
                    } => self.reduce(spec, sources, layout, *partition).await?,
                    other => {
                        let input = match other {
                            Input::Inline(bytes) => bytes.clone(),
                            Input::Object(r) => self.store.get(r).await?,
                            Input::ObjectSlice(slice) => {
                                slice.validate()?;
                                if slice.length > loom_shuffle::MAX_PAYLOAD_BYTES as u64 {
                                    return Err(Error::Invalid(
                                        "task input slice exceeds 4 MiB".into(),
                                    ));
                                }
                                let bytes = self
                                    .store
                                    .get_range(&slice.object, slice.offset, slice.length)
                                    .await?;
                                if bytes.len() as u64 != slice.length {
                                    return Err(Error::Invalid(
                                        "object provider returned an incomplete range".into(),
                                    ));
                                }
                                bytes
                            }
                            _ => return Err(Error::Invalid("unresolved task dependency".into())),
                        };
                        self.engine
                            .task(&spec.module, &spec.function, input)
                            .await?
                    }
                };
                // Blobs are immutable; only the fenced commit publishes the output reference.
                let output = match &spec.output {
                    TaskOutput::Value => self.store.put(bytes).await?,
                    TaskOutput::Shuffle(layout) => {
                        loom_shuffle::publish(self.store.as_ref(), layout, &spec.id, &bytes).await?
                    }
                };
                Ok(Completion::Task { output })
            }
            Work::Actor { snapshot, message } => {
                let decision = self
                    .engine
                    .actor(
                        &snapshot.spec.module,
                        &snapshot.spec.handler,
                        snapshot.state.clone(),
                        message.payload.clone(),
                    )
                    .await?;
                Ok(Completion::Actor {
                    expected_revision: snapshot.revision,
                    decision,
                })
            }
        }
    }
    async fn reduce(
        &self,
        spec: &TaskSpec,
        sources: &[ShuffleSource],
        layout: &ShuffleLayout,
        partition: u32,
    ) -> Result<Bytes> {
        layout.validate()?;
        if partition >= layout.partitions || sources.len() > MAX_SHUFFLE_SOURCES {
            return Err(Error::Invalid("invalid shuffle reducer input".into()));
        }
        let mut state = None;
        for source in sources {
            let ShuffleSource::Manifest(reference) = source else {
                return Err(Error::Invalid("unresolved shuffle dependency".into()));
            };
            let manifest: ShuffleManifest =
                loom_shuffle::decode(&self.store.get(reference).await?)?;
            loom_shuffle::validate_manifest(&manifest, layout)?;
            for block in &manifest.partitions[partition as usize] {
                let records =
                    loom_shuffle::read_block(self.store.as_ref(), block, layout, partition).await?;
                let frame = loom_shuffle::encode(&ReduceFrame { state, records })?;
                state = Some(
                    self.engine
                        .task(&spec.module, &spec.function, frame)
                        .await?,
                );
            }
        }
        match state {
            Some(bytes) => Ok(bytes),
            None => {
                self.engine
                    .task(
                        &spec.module,
                        &spec.function,
                        loom_shuffle::encode(&ReduceFrame {
                            state: None,
                            records: vec![],
                        })?,
                    )
                    .await
            }
        }
    }
    pub async fn tick(&self) -> Result<bool> {
        let Some(claim) = self.store.claim(&self.session, self.lease_ms).await? else {
            return Ok(false);
        };
        eprintln!(
            "CLAIM {} epoch={} attempt={}",
            claim.fence.job, claim.fence.epoch, claim.attempt
        );
        let execution = self.execute(&claim);
        tokio::pin!(execution);
        // Poll BOTH futures while either awaits the backend. Awaiting renewal inside
        // a select branch can deadlock if execute currently owns a connection mutex.
        let renewal = async {
            loop {
                tokio::time::sleep(Duration::from_millis((self.lease_ms / 3).max(1))).await;
                self.store.renew_claim(&claim, self.lease_ms).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        };
        tokio::pin!(renewal);
        let result = tokio::select! {
            result = &mut execution => result,
            lost = &mut renewal => { lost?; return Err(Error::Fenced); }
        };
        match result {
            Ok(result) => {
                // Do not turn an unknown commit outcome into failure/re-execution.
                match self.store.commit(&claim, result).await {
                    Ok(_) => eprintln!("COMMIT {}", claim.fence.job),
                    Err(error @ (Error::Invalid(_) | Error::NotFound(_) | Error::Conflict(_))) => {
                        self.store.fail(&claim, error.to_string()).await?;
                        eprintln!("RESTART {}: {error}", claim.fence.job);
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => {
                self.store.fail(&claim, error.to_string()).await?;
                eprintln!("RESTART {}: {error}", claim.fence.job);
            }
        }
        Ok(true)
    }
    /// Compatibility entry point with one execution slot.
    pub async fn run(&self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        self.run_with_slots(1, shutdown).await
    }

    /// Slots share one node session and heartbeat. Each claim renews independently.
    /// Dropping this future aborts all polling/heartbeat tasks; leases then expire.
    pub async fn run_with_slots(
        &self,
        slots: usize,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<()> {
        if !(1..=256).contains(&slots) {
            return Err(Error::Invalid("node slots must be 1..256".into()));
        }
        self.register().await?;
        enum Event {
            Heartbeat(Result<()>),
            Slot(Result<()>),
            Drained(Result<()>),
        }
        // JoinSet owns every spawned task, including the heartbeat, so cancellation
        // cannot leave an orphan heartbeat keeping abandoned claims alive forever.
        let mut workers = tokio::task::JoinSet::new();
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let node = self.clone();
        workers.spawn(async move {
            let result = async {
                loop {
                    tokio::time::sleep(Duration::from_millis((node.lease_ms / 3).max(1))).await;
                    node.store.renew(&node.session, node.lease_ms).await?;
                }
                #[allow(unreachable_code)]
                Ok(())
            }
            .await;
            Event::Heartbeat(result)
        });
        for _ in 0..slots {
            let node = self.clone();
            let stopped = stopped.clone();
            workers.spawn(async move { Event::Slot(node.run_slot(stopped).await) });
        }
        tokio::pin!(shutdown);
        let mut draining = false;
        let mut remaining = slots;
        let result = loop {
            tokio::select! {
                _ = &mut shutdown, if !draining => {
                    draining = true;
                    let node = self.clone();
                    // Keep observing heartbeats and slot errors while drain waits for storage.
                    workers.spawn(async move {
                        Event::Drained(node.store.drain(&node.session).await)
                    });
                }
                event = workers.join_next() => {
                    match event {
                        Some(Ok(Event::Drained(Ok(())))) => { let _ = stop.send(true); }
                        Some(Ok(Event::Slot(Ok(())))) => {
                            remaining -= 1;
                            if remaining == 0 { break Ok(()); }
                        }
                        Some(Ok(Event::Slot(Err(error)) | Event::Drained(Err(error)) | Event::Heartbeat(Err(error)))) => break Err(error),
                        Some(Err(error)) => break Err(Error::Unavailable(format!("node worker failed: {error}"))),
                        _ => break Err(Error::Unavailable("node heartbeat ended unexpectedly".into())),
                    }
                }
            }
        };
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        result
    }

    async fn run_slot(&self, mut stopped: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        while !*stopped.borrow() {
            let tick = self.tick();
            tokio::pin!(tick);
            let worked = tokio::select! {
                biased;
                _ = stopped.changed() => {
                    // Drain has committed: finish any concurrent claim/execution, but
                    // never drop a claim response whose outcome is still in flight.
                    tick.await?;
                    return Ok(());
                }
                result = &mut tick => result?,
            };
            if !worked {
                tokio::select! {
                    _ = stopped.changed() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(50)) => (),
                }
            }
        }
        Ok(())
    }
}
