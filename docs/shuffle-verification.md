# Shuffle protocol verification

The protocol was checked through public backend contracts, runtime execution, and
real Rust/Wasm tasks in a two-worker Compose environment.

## Random default update (protocol version 2)

New layouts use `ShuffleLayout::new(partitions)` for Random; Word Count explicitly
uses `ShuffleLayout::hash(partitions)`. The updated shuffle suite has ten default
tests and one opt-in PostgreSQL test, all passing. Added coverage verifies:

- 4,096 records with the same key spread across all four partitions, with every
  unique value preserved exactly once. Publishing the same task/output again
  produces an identical manifest; a different task ID changes the assignment.
- Sixty-four distinct single-record maps also spread across all partitions;
  identical keys and ordinal zero do not force a shared destination.
- Two mapper tasks and four reducers execute through two worker sessions against
  both memory and PostgreSQL coordination, preserving the combined total.
- Random layouts survive serialization. Old layouts without a strategy retain
  Hash semantics, and version 1 Hash manifests remain readable. Invalid version 1
  Random manifests and mismatched mapper/reducer strategies are rejected.

Workspace compilation, formatting, Clippy with warnings denied, and host tests
passed. The Linux container suite and all explicit integrations passed **26
behavior/integration tests plus one generated doctest**. The real Wasm actor demo
and all 11 Word Count acceptance groups also passed with the rebuilt images.
Word Count still independently verifies key routing using Python SHA-256 and the
new version 2 manifests with explicit Hash layouts.

Random runtime coverage uses synthetic guests to isolate routing behavior; real
Wasm Word Count exercises Hash mode. The two worker sessions in each contract
test share one process. These checks establish correctness, not a throughput or
load-balancing benchmark. Sorting and crash/recovery acceptance were not rerun
for this update; their earlier results follow below.

## Initial Hash implementation verification

`crates/conformance/tests/shuffle.rs` covers eight default tests and one opt-in
PostgreSQL test in the initial implementation, all passing at that revision:

1. Fixed SHA-256 partition vectors and deserialization of old TaskSpec records with
   default ordinary-value output.
2. Opaque non-UTF-8 keys/values, empty keys, deterministic manifests, preservation
   of every record, multi-block partitions, and rejection of oversized records.
3. Reducer dependency barrier, mapper lease expiry, stale commit fencing, exact
   commit retry, and distinct mappers with identical manifests counted twice.
4. Failed dependency propagation even while another mapper remains busy, without
   consuming a reducer attempt.
5. Duplicate/missing dependencies, invalid partitions/layouts, and excessive source
   counts rejected before enqueueing.
6. A reducer succeeds with a deliberately nonexistent block in an unselected
   partition; wrong routing in a selected partition and bad manifest versions fail.
7. Injected reducer failure after a partial fold; retry starts fresh and produces
   the correct total. Empty input produces initialized reducer state.
8. Injected failure during mapper block publication returns no partial manifest;
   a retry publishes a complete readable result.
9. The dependency/fencing/identical-manifest scenario against PostgreSQL, followed
   by reconnection and verification of the committed reducer output.

These use synthetic engines for precise failure boundaries. Actual Wasm behavior
and physical worker execution are covered by the Word Count acceptance harness,
including 100 Maps and 4 Reducers over 50,000 words. See
[word-count-verification.md](word-count-verification.md).

## Regression checks

- Formatting and workspace Clippy with warnings denied: passed.
- Linux workspace suite: 17 behavior/regression tests plus one generated doctest
  passed; three environment-dependent tests skipped in the ordinary invocation.
- Explicit PostgreSQL/Wasm integration and PostgreSQL shuffle invocation: all
  three passed, for **20 behavioral/integration tests** in total.
- Rust/Wasm research-agent demo: passed.
- Sorting acceptance: all six scenario groups passed.
- Compose crash/recovery acceptance: all six scenario groups passed, including
  SIGKILL takeover, committed-work preservation, actor recovery/deduplication,
  database-container recreation, and fuel-trap isolation.

Shuffle failure injection is not a claim that a multi-machine network partition
or an actual external side effect was tested. The Compose fault suite exercises
process/container failure in the shared runtime; the targeted shuffle tests
control lease and publication boundaries through the contracts.

## Reproduce

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/local-env.sh docker compose --profile tests build node-a tests
# Upgrade all workers before submitting new shuffle records.
scripts/local-env.sh docker compose stop node-a node-b
sleep 6
scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/word_count_acceptance.py --report docs/word-count-acceptance.json
scripts/local-env.sh python3 scripts/sort_acceptance.py
scripts/local-env.sh python3 scripts/compose_acceptance.py
```
