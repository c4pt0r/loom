# Shuffle protocol verification

The protocol was checked through public backend contracts, runtime execution, and
real Rust/Wasm tasks in a two-worker Compose environment.

## Behavioral tests

`crates/conformance/tests/shuffle.rs` covers eight default tests and one opt-in
PostgreSQL test, all passing:

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
