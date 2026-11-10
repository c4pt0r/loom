# Object storage verification

The independent storage interfaces were verified through contract tests, real
Wasm execution, and the updated two-worker Docker Compose environment.

## Storage isolation

`crates/conformance/tests/object_store.rs` covers four passing scenarios:

1. Immutable binary objects round-trip, identical writes return the same digest,
   clones share a namespace, and independent memory-store instances are isolated.
2. Map and reduce tasks use an independently injected object store for modules,
   inputs, shuffle blocks/manifests, and results. None of these payloads appears
   in the coordination adapter's own blob storage.
3. An unavailable object provider reports failure even when the coordination
   adapter contains the requested object. Membership remains operational, and
   task execution does not silently fall back to that adapter's blobs.
4. PostgreSQL coordinates two dependent tasks executed through real Wasm and two
   worker sessions, with a separate memory object provider. A second PostgreSQL
   connection reads the committed task state; both tasks complete in one attempt,
   and module/input/output bytes remain absent from PostgreSQL blob storage.

The fourth scenario uses two logical workers in one process sharing the same
memory object store. It does not demonstrate durable or cross-process memory
storage. The Compose examples use PostgreSQL for both storage roles.

## Regression results

- Workspace formatting, compilation, and Clippy with warnings denied: passed.
- Host workspace tests: 20 behavior tests and one generated doctest passed; four
  environment-dependent tests were skipped in the ordinary invocation.
- Linux container workspace suite and all four explicit PostgreSQL/Wasm tests:
  **24 behavior/integration tests plus one generated doctest passed**.
- Rust/Wasm research-agent demo: passed.
- Word Count acceptance: all 11 scenario groups passed, including 50,000 words
  processed by 100 Maps and 4 Reducers across the two worker containers.
- Sorting acceptance: all six scenario groups passed.

No S3 adapter, object garbage collection, or multi-machine network fault behavior
is claimed by these checks. The existing crash/recovery suite was not rerun for
this interface change; its previous results are recorded in
[shuffle verification](shuffle-verification.md).

## Reproduce

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/local-env.sh docker compose --profile tests build node-a tests
scripts/local-env.sh docker compose stop node-a node-b
sleep 6
scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/word_count_acceptance.py
scripts/local-env.sh python3 scripts/sort_acceptance.py
```

The container test script explicitly runs the otherwise ignored PostgreSQL and
Wasm tests using its bundled component and Compose database configuration.
