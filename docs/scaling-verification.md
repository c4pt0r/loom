# Coordination and object-slice verification

The row-oriented PostgreSQL adapter and object-slice inputs were checked on the
host and in rebuilt Linux images, followed by real two-worker Compose acceptance.

## Focused checks

`crates/conformance/tests/postgres_rows.rs` uses isolated namespaces and deliberate
SQL lock fixtures, then verifies behavior through the public backend interfaces:

- Hold one task row and the old namespace ledger row locked. Another task can be
  claimed and committed; task status reads and an unrelated submission finish
  while those locks remain held.
- Eight concurrent worker sessions claim and commit 50 pending jobs through one
  shared adapter. No job is claimed twice, each new task has one attempt, and
  repeated exact commits return the original receipt.
- A schema version 1 fixture migrates with actor state, completed results, pending
  dependencies, attempts, and commit receipts preserved. A new worker completes
  the remaining task, and reconnecting observes its result.
- PostgreSQL range reads return the exact requested bytes, reject out-of-bounds
  requests, allow an empty range at EOF, and preserve complete-object reads.

`crates/conformance/tests/object_slices.rs` verifies that the runtime invokes the
range interface without downloading the complete source, while complete-object
inputs still use their existing behavior. It also checks serialized slice inputs,
legacy-adapter fallback, opaque binary data, missing objects, range overflow, and
strict bounds.

The original actor/task contract tests run against the new PostgreSQL adapter:
atomic rollback and outbox publication, receipt deduplication, session and lease
fencing, dependency barriers, retry budgets, actor ordering, and reconnects.

## Executed results

| Check | Result |
| --- | --- |
| Workspace formatting, compilation, Clippy with warnings denied | PASS |
| Host workspace suite | 24 behavior tests and one generated doctest passed; six database/Wasm tests intentionally ignored |
| Linux workspace suite plus every explicit integration | 30 behavior/integration tests and one generated doctest passed |
| Real Wasm research-agent demo | PASS |
| New object-slice Compose acceptance | All four scenario groups passed |
| Existing Word Count acceptance | All 11 scenario groups passed |
| Existing sorting acceptance | All six scenario groups passed |
| Compose fault/recovery acceptance | All six scenario groups passed |

The new Compose checks cover multi-byte UTF-8 slices, a reference-only driver,
complete raw-object inputs, compatibility with the existing stdin mode, empty
manifests, and failure on an out-of-bounds range. Expected counts are computed
independently in Python.

The fault suite kills worker containers, verifies takeover of uncommitted work
without replaying committed tasks, recovers an actor after both workers disappear,
recreates the database container while retaining its volume, and checks Wasm trap
isolation. It uses a separate Compose project and cleans up that project's data.

These results establish the tested contract behavior and demonstrate that an
unrelated row lock no longer blocks scheduling. They are not a throughput
benchmark, a claim of linear speedup, or proof of multi-machine partition tolerance.

## Reproduce

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/local-env.sh docker compose --profile tests build node-a tests
# Stop all old workers before the first version 2 database connection.
scripts/local-env.sh docker compose stop node-a node-b
sleep 6
scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/object_slices_acceptance.py
scripts/local-env.sh python3 scripts/word_count_acceptance.py
scripts/local-env.sh python3 scripts/sort_acceptance.py
scripts/local-env.sh python3 scripts/compose_acceptance.py
```
