# Local Docker Compose environment

## Services

| Service | Purpose | Started by default |
| --- | --- | --- |
| `postgres` | PostgreSQL 17; named persistent data volume | yes |
| `node-a` | Rust host, one slot by default, node ID `node-a` | yes |
| `node-b` | Rust host, one slot by default, node ID `node-b` | yes |
| `dashboard` | Read-only web console, loopback port 8787 | `dashboard` profile |
| `cli` | One-shot CLI, bundled Wasm component | `tools` profile |
| `sort` | Rust distributed-sort client and bundled sorting Wasm | `tools` profile |
| `word-count` | Rust word-count driver and bundled counting Wasm | `tools` profile |
| `tests` | Linux Rust test binaries, PostgreSQL/Wasm integration, demo | `tests` profile |
| `control` | Persistent CLI runner for acceptance tests | `tests` profile, harness only |
| `claim-fixture` | Deterministically hold a lease before SIGKILL | `tests` profile, harness only |

The runtime image is built from source using a pinned Rust toolchain tag and
`Cargo.lock`, then copied into Debian slim. It runs as UID/GID 10001, with no source
or Cargo toolchain. Wasm components are compiled inside Linux as part of the image
build; host `target` and generated components are excluded from the build context.

The test image retains the toolchain and compiled release test binaries. It runs
Cargo offline after image build. Native images follow the Docker engine's architecture;
the same Wasm guest artifact is portable within the supported host ABI.

## Start and inspect

From the repository root:

```sh
docker compose --profile tests build node-a tests
docker compose up -d --wait postgres node-a node-b
docker compose ps
docker compose logs --tail 50 node-a node-b
docker compose --profile tools run --rm cli --namespace lab members
```

`--wait` ensures the database is healthy and workers are running. Use `members` to
verify both workers have registered. Workers share the `lab` namespace unless
`LOOM_NAMESPACE` is set. The five-second node leases are independent of work leases.
Normal shutdown uses SIGINT so Loom can drain the current turn. Restarting a node
with the same ID requires its previous incarnation's lease to expire.

PostgreSQL uses local-development credentials in `compose.yaml`; it has no host port
mapping. Clients, workers, and integration tests use the URI DSN
`postgresql://loom:loom-local-only@postgres:5432/loom` via `LOOM_DATABASE_URL`
(and `LOOM_TEST_DATABASE_URL` for tests). Percent-encode reserved characters in
credentials when supplying your own DSN. To inspect the database:

```sh
docker compose exec postgres psql -U loom -d loom
```

See the [dashboard guide](dashboard.md) to inspect this cluster in a browser.

## Run the Rust/Wasm actor

For a task-only example, see the [distributed sorting Quickstart](quickstart-sort.md).

```sh
docker compose --profile tools run --rm cli --namespace lab deploy /opt/loom/research-agent.component.wasm
# Copy the returned digest into MODULE_DIGEST:
docker compose --profile tools run --rm cli --namespace lab create-actor research MODULE_DIGEST
docker compose --profile tools run --rm cli --namespace lab send research --id start \
  '{"start":["Rust Wasm actors","Durable execution"]}'
docker compose --profile tools run --rm cli --namespace lab actor research
# Once awaiting_approval:
docker compose --profile tools run --rm cli --namespace lab send research --id approval '{"approve":true}'
docker compose --profile tools run --rm cli --namespace lab actor research
```

Deploy and execute in the same namespace; blobs are namespace-scoped.

## Tests

```sh
docker compose --profile tests run --rm tests
python3 scripts/compose_acceptance.py --report docs/compose-acceptance.json
```

The harness uses a generated project name `loom-test-<id>` and a generated namespace.
Its database volume is independent of the development stack. It verifies:

1. Healthy database and isolated network/volume setup.
2. SIGKILL of a container holding an uncommitted task claim, followed by takeover.
3. Previously committed task result and attempt count do not change.
4. Two worker containers execute tasks and a persistent actor aggregates eight results.
5. Both workers can be killed while waiting for approval; restarted workers recover
   the actor and deduplicate repeated approval.
6. Recreating the PostgreSQL container retains state, receipts, modules and result
   blobs in its named volume.
7. A guest fuel-exhaustion trap fails the task while workers continue processing.

The deterministic claim fixture is intentionally killed before execution. This
does not test external exactly-once effects or pretend an external API call was
interrupted. The runtime has no external tool capabilities yet.

The harness records PASS/FAIL and diagnostics as JSON when `--report` is supplied,
and cleans up its uniquely named project, including its test volume, on exit.

## Workspace-local Docker on this Mac

The initial environment uses Colima with Apple Virtualization, 8 CPUs, 12 GiB RAM,
and a 40 GiB data disk. VM state is under the enclosing workspace's `work/colima`;
Docker CLI configuration is under `work/docker-config`. No global Docker context
or shell profile changes are required.

Use the wrapper, which selects the local socket when present and otherwise leaves
your existing Docker configuration alone:

```sh
scripts/local-env.sh docker compose ps
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/compose_acceptance.py --report docs/compose-acceptance.json
```

If that workspace-local VM was stopped, restart it from the repository root:

```sh
COLIMA_HOME="$(cd ../../work/colima && pwd)" \
DOCKER_CONFIG="$(cd ../../work/docker-config && pwd)" \
colima start loom --activate=false --ssh-config=false
```

Docker CLI, Compose, Buildx and Colima were installed with Homebrew. Docker Desktop
or another configured Docker engine can also run these files without the wrapper.

## Stop and retain data

```sh
docker compose down
```

This removes the development containers/network and retains its PostgreSQL volume.
`docker compose down --volumes` also deletes that project's data; use only when
intentionally resetting the development cluster.

## Multiple execution slots

Set `LOOM_SLOTS=4` when starting or recreating workers to enable four slots per
node. Tasks and actor turns share those slots; each actor remains single-writer.
See [concurrent actors](multi-actor.md) for restart commands, shutdown behavior,
and the independent multi-actor acceptance harness.
