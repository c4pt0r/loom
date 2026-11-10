# Initial verification record

This records the original ledger-based revision. See the
[current coordination and input verification](scaling-verification.md) for the
row-oriented PostgreSQL implementation and updated test counts.

The later [Docker Compose run](compose-verification.md) also verifies the release
build inside Linux containers, container SIGKILL recovery and PostgreSQL volume persistence.

Executed locally on 2026-11-09, macOS ARM64, Rust 1.97.1, Wasmtime 45.0.3,
PostgreSQL 17. A dedicated local test cluster and fresh namespaces were used.

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace` | 9 behavior/regression tests and 1 generated binding doctest passed; 2 environment-dependent tests intentionally ignored in this command |
| Explicit PostgreSQL and Wasm tests (`--ignored`) | Both passed; none skipped in this invocation |
| Rust guest build and Component encoding | PASS |
| Local Wasm research/approval demo | PASS |
| Multi-process recovery acceptance | All 6 checks passed |
| Runtime dependency isolation | Direct production dependencies: `loom-contracts`, `tokio`; no database adapter or engine dependency |

## Behavior coverage

- Atomic state/receipt/task-intent/message commit, including rollback when a later
  outgoing message is invalid.
- Same-ID request deduplication and conflicting-payload rejection.
- Exact commit replay and immutable completed outcomes.
- Expired work fencing and monotonically increasing attempt epochs.
- Concurrent claim contention: one winner, including separate PostgreSQL connections.
- Actor serialization when an earlier timer becomes ready during another turn.
- Node incarnation conflict, expiry, stale renewal rejection and draining.
- Task dependency resolution through immutable content-addressed blobs.
- Finite actor restart budget and poison-message quarantine.
- Wasm fuel exhaustion, explicit guest error, and successful execution after a trap.
- Lease renewal and result publication contending for the same backend connection
  continue making progress.

## Process recovery

Command: `python3 scripts/process_recovery.py`, with `LOOM_DATABASE_URL` set.
Successful namespace: `process-60892a9c4851419bad22b266bcb3eb62`.

1. Complete a baseline task on the first worker and preserve its result/attempt count.
2. A test fixture acquires another task's lease, then is killed with SIGKILL before
   execution. This is a deterministic uncommitted-claim failure, not a claim that
   an external side effect was interrupted.
3. Replacement production workers take over and execute the real Wasm task.
   The interrupted task has exactly two attempts; the committed task remains at one.
4. Two production workers share task execution and a persistent actor, which
   aggregates two immutable result references and waits for approval.
5. Kill both workers, start a new worker, and send approval twice with the same ID.
6. The actor reaches `done` at revision 4; its report references are unchanged.

## Defect found and fixed during verification

The first PostgreSQL process test stalled while publishing task outputs. Work
execution could hold the adapter's connection mutex while the runtime selected a
lease-renewal branch and awaited that same mutex without polling execution.

Renewal and execution now run as concurrently polled futures. The test
`publication_and_renewal_must_progress_concurrently` forces the same contention
with a slow publication and verifies completion within a bounded deadline.
The complete process test passed after this fix.

## What this does not establish

These tests validate the prototype's contracts for the exercised scenarios. They
do not establish production scalability, cross-region availability, external
exactly-once effects, hostile multi-tenant isolation or native-equivalent speed.
No performance comparison against Ray/native code has been run. The reference
PostgreSQL adapter serializes a namespace through one ledger row.
