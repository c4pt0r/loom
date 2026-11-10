# Dashboard

Loom includes a read-only web console for a single backend namespace. It shows real
persisted state; opening the page never submits work, retries jobs, or starts an
agent. HTML, CSS, and JavaScript are embedded in the Rust binary. No Node.js build,
external CDN, or separate frontend service is needed.

## Start with Docker Compose

From the repository root:

```sh
scripts/local-env.sh docker compose --profile dashboard build dashboard
scripts/local-env.sh docker compose --profile dashboard up -d --wait --no-recreate postgres node-a node-b dashboard
```

Open **http://127.0.0.1:8787**. The default namespace is `lab`; configure
`LOOM_NAMESPACE` before starting the service to select another one. Use
`LOOM_DASHBOARD_PORT` to change the published port. The service is behind the
`dashboard` profile and its published port is bound to loopback.

## Start a native dashboard

```sh
cargo build --locked -p loom-cli
export LOOM_DATABASE_URL='postgresql://loom:loom-local-only@127.0.0.1:15432/loom'
target/debug/loom --namespace book-summary dashboard --bind 127.0.0.1:8788
```

This connects to the separate PostgreSQL stack used by the
[local Pi example](../examples/book-summary/README.md). Open
**http://127.0.0.1:8788** to inspect chapter tasks, the coordinator actor, and the
final report. Native dashboard defaults to `127.0.0.1:8787`; explicitly select the
correct namespace and storage DSN. Multiple consoles can observe different
backends/namespaces on different ports. There is no cross-namespace selector yet.

## What to inspect

- **Overview:** active nodes, running/pending/failed tasks, and task-state totals.
  Counts cover the namespace, independent of the current search or page.
- **Tasks:** search by ID, function, or notification actor; filter by state and
  paginate. Open a row for its attempt budget, owner, input, receipt, and failure
  reason. Successful tasks expose their immutable result directly.
- **Actors:** application phase when available, durable state revision, pending
  messages, and active turns. Open the committed state and follow ObjectRefs to
  source data, chapter summaries, or a final report. Application phase is distinct
  from execution status: an actor with phase `failed` may have no failed message.
- **Nodes:** online, draining, and expired registrations, incarnation ID, lease
  deadline, and active job count. Expired does not prove the host is physically
  down. A completed task's owner is historical, not a claim that it is still live.
- **Messages:** actor-turn history, including failed messages and commit receipts.
  Messages are counted separately from computation tasks.

The page refreshes every three seconds while visible. Pause auto-refresh for
inspection, or refresh manually. Backend failures retain the previous snapshot
with an explicit stale-data banner; stale data is never presented as confirmed
current health. Detail drawers are snapshots taken when opened; reopen to refresh.

## Status semantics

| State | Meaning |
| --- | --- |
| Queued | No attempt yet; eligible for a worker. |
| Running | Both work and current node-incarnation leases are live. |
| Waiting | Waiting for dependencies, another actor turn, or scheduled time. |
| Retrying | A previous attempt ended or lost its lease; no live execution owns it. |
| Succeeded | A success receipt was committed. |
| Failed | A failure receipt was committed. |

A retrying job may be in backoff, awaiting takeover, or awaiting failure
finalization after using its attempt budget. A failed dependency remains `waiting`
until the scheduler commits the dependent failure. Reads do not perform these
transitions. New worker versions persist the latest attempt error; older records
may only contain a terminal error. A successful retry can retain its prior error
for inspection.

## Interfaces and API

The `loom-dashboard` crate depends on `InspectionStore` and `ObjectStore`, not a
PostgreSQL client. `InspectionStore` is separate from mutation interfaces and has
memory and PostgreSQL implementations. PostgreSQL reads use a read-only,
repeatable-read snapshot, backend-authoritative time, and no execution row locks.
Global totals scan namespace metadata; only the requested page's job/actor bodies
are returned. Page size is 1–100, default 25. Large histories still need future
indexing/rollups for cheap global totals; this is not a metrics time-series system.

| GET endpoint | Purpose |
| --- | --- |
| `/api/config` | Namespace and refresh settings. |
| `/api/inspect?resource=tasks` | Summary and paginated tasks, messages, actors, or nodes. |
| `/api/job?id=task:ID` | Task or message details using the full job ID. |
| `/api/actor?id=ID` | Actor state and mailbox counts. |
| `/api/object?digest=SHA256` | Bounded object preview. |

Inspection query parameters include `search`, `phase`, `offset`, and `limit`.
Object previews use `ObjectStore::size` and `get_range`, reading at most 64 KiB;
binary output is shown as a 256-byte hex preview. Input and actor detail previews
are also capped at 64 KiB. Truncation and total byte lengths are visible. Providers
without native metadata/range support may use the compatibility full-read fallback.

The console is intended for a trusted local environment: there is no login or
role system. Keep its default loopback binding. Responses are non-cacheable, data
is rendered as text, and the API exposes no mutation routes. The host header is
restricted to localhost, loopback, or the explicit bind IP. Browsing through an
arbitrary reverse-proxy hostname is not supported by this first version.

## Verification

```sh
cargo test -p loom-dashboard
cargo test -p loom-conformance --test inspection
LOOM_TEST_DATABASE_URL="$LOOM_DATABASE_URL" \
  cargo test -p loom-conformance --test inspection -- --ignored
```

Tests cover projection parity across memory and PostgreSQL, pagination, literal
search, namespace isolation, live/expired/replaced node incarnations, retry error
persistence, actor mailbox counts, HTTP validation, read-only routes, and bounded
result previews. Existing scheduling and recovery conformance tests also run with
the new optional error field. CPU, memory, slot utilization, duration percentiles,
and streamed process logs are not yet collected, so the UI does not invent them.

See the [verification record](dashboard-verification.md) for exercised scenarios.
