# Distributed sorting Quickstart verification

Environment: local Colima Linux ARM64 VM, PostgreSQL 17, two worker containers
(`node-a`, `node-b`), freshly built `loom-local:dev` image, namespace `lab`.

The Rust guest was compiled into a Wasm Component inside the Docker build. The
Rust client was executed in one-shot Compose containers against the running workers.

## Results

| Check | Result |
| --- | --- |
| Default 16,000 integers / 16 tasks | PASS; both workers committed tasks from this run |
| Custom eight integers / three tasks | PASS; `[-8,-2,0,1,3,7,7,9]` |
| Empty array | PASS; empty result, no tasks |
| Eleven identical integers / four tasks | PASS; duplicates preserved |
| Signed 64-bit minimum and maximum | PASS; exact values preserved |
| Floating point and out-of-range input | PASS; rejected with nonzero exit status |
| Read final custom result using `loom get` | PASS; persisted array matches expected output |

For all five successful inputs, an external Python harness compared the entire
returned array with Python's `sorted`, checked element/task counts, and confirmed
each task completed in one attempt. The default case also matched its exact task
IDs against each worker's commit logs. This is additional to the Rust client's own
reference-sort verification.

Machine-readable results and task-run identifiers: [sort-acceptance.json](sort-acceptance.json).

Commands used:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --locked --workspace
scripts/local-env.sh docker compose --profile tools config --quiet
scripts/local-env.sh docker compose --profile tools build node-a
scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
scripts/local-env.sh python3 scripts/sort_acceptance.py --report docs/sort-acceptance.json
```

Workspace tests passed: eight in-memory contract tests, the publication/renewal
regression test, and one generated binding doctest. The two opt-in conformance
tests remain ignored in this command; this run exercises PostgreSQL and Wasm
through the actual sorting application instead.

This verifies the Quickstart's behavior on two containers in one VM. It does not
measure speedup, large-data scalability, or sorting-specific crash recovery.
Final k-way merge and the reference-sort oracle run in client memory. The two
worker containers and database remain running after verification.
