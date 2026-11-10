# Docker Compose verification

Verified on 2026-11-09 (America/Los_Angeles).

## Environment

- macOS ARM64 host; dedicated Colima profile `loom`, Apple Virtualization backend.
- VM: 8 vCPUs, 12 GiB RAM, 40 GiB data disk.
- Docker CLI 29.9.3; Docker Engine 29.5.2; Compose 5.7.0; Buildx 0.38.2.
- Container target: Linux ARM64, Rust 1.93, Wasmtime 45.0.3, PostgreSQL 17.
- Runtime image `loom-local:dev`: approximately 170 MB as reported by Docker.
- Development Compose project `loom-local` remains running: PostgreSQL and two nodes.
- No host database port is published. CLI access uses the Compose network.

## Results

| Check | Result |
| --- | --- |
| Compose configuration validation | PASS |
| Linux runtime + test image builds from source | PASS |
| Release workspace tests inside container | 9 behavior/regression tests and 1 generated binding doctest passed |
| Explicit PostgreSQL and Wasm integration tests | 2 passed; no skipped tests in this invocation |
| Rust/Wasm research-and-approval demo inside container | PASS |
| Actual Compose-container acceptance | All 6 scenario groups passed |
| Development cluster membership | `node-a` and `node-b` registered, not draining |
| Temporary test project cleanup | Removed; only `loom-local` remains |

## Container acceptance scenarios

Test project: `loom-test-9c2b05c0b185`.
Namespace: `acceptance-9c2b05c0b185`.

1. PostgreSQL became healthy with an isolated network and persistent volume.
2. A container holding an uncommitted task claim was killed with SIGKILL. A worker
   took over with attempt count 2. A previously committed task remained at attempt 1.
3. Both worker containers executed Wasm tasks. The actor aggregated eight object
   references; every referenced analysis result was fetched and checked.
4. Both worker containers were killed while the actor awaited approval. Restarted
   workers recovered it, and two identical approval messages produced one update
   (final revision 10).
5. The PostgreSQL container was force-recreated using the same named volume. Actor
   state, task outcomes, attempt counts and result blobs were unchanged. New worker
   processes subsequently loaded the persisted Wasm module and executed more work.
6. An infinite-loop guest exhausted its fuel and failed its task. Both workers
   remained running and a subsequent task completed successfully.

The claim fixture is deliberately killed before guest execution to make takeover
deterministic. No assertion is made about arbitrary external effects or a live
external request being interrupted. This run tests containers on one local VM;
it is not a multi-machine network-partition test or a performance benchmark.

Machine-readable evidence: [compose-acceptance.json](compose-acceptance.json).
Reproduction commands and lifecycle instructions: [compose.md](compose.md).

## Commands used

```sh
scripts/local-env.sh docker compose --profile tests build node-a tests
scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/compose_acceptance.py --report docs/compose-acceptance.json
scripts/local-env.sh docker compose --profile tools run --rm -T cli --namespace lab members
scripts/local-env.sh docker compose ls --format json
```

The first test command intentionally skips the two tests requiring environment
variables. The next command inside the test container explicitly runs those two
tests with the PostgreSQL and Wasm paths configured. All requested tests finished.
