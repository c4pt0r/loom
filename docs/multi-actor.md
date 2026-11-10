# Concurrent actors and node slots

Different actors can execute concurrently. A single actor still processes one
message turn at a time, with state and outgoing intents committed atomically.
Actors do not permanently occupy a worker or a Wasm instance: an actor waiting for
a message, task result, timer, or approval exists as durable storage records.

## Configure execution capacity

```sh
loom --namespace lab node --id node-a --slots 4
```

`--slots` accepts 1–256 and defaults to 1. A slot executes one task or one actor
message turn, including input/output IO and commit. Tasks and actors share this
capacity. Two nodes with four slots each can have up to eight claims executing.
More actors can remain queued without consuming execution slots.

For the local Compose environment:

```sh
scripts/local-env.sh docker compose stop node-a node-b
sleep 6
LOOM_SLOTS=4 scripts/local-env.sh docker compose up -d --wait postgres node-a node-b
```

Keep `LOOM_SLOTS=4` set when later recreating these workers. Omitting it restores
the default of one slot. The stop/wait sequence lets old node registrations expire
before a replacement starts with the same node ID.

Rust hosts can choose the capacity without changing the existing `Node` struct:

```rust
node.run_with_slots(4, shutdown).await?;
```

`node.run(shutdown)` remains compatible and selects one slot. `tick()` still
executes at most one claim for embedding and deterministic tests.

## Ownership and shutdown

All slots on one node share a node incarnation and one membership heartbeat. Each
claimed job independently renews its work lease while executing. Database locks
are held only for coordination transactions, not throughout guest computation.

Per-actor scheduling and fencing still provide single-writer state commits across
all nodes and slots. After lease expiry an old computation may physically overlap
with recovery, but its stale claim cannot commit. This is not an external
exactly-once side-effect guarantee.

A graceful shutdown marks the node as draining before signaling its slots to
stop. No new work is admitted after that storage transition. Existing slots finish
while membership and work leases continue renewing. A membership/slot failure
stops the node's remaining polling tasks. Dropping or cancelling the runner also
cancels its heartbeat; it cannot keep abandoned claims alive in the background.
An already running synchronous Wasm invocation may finish within its fuel bound,
but its cancelled orchestration does not proceed to commit.

The process supervisor's shutdown timeout still applies. Forced process loss uses
the existing lease expiry and retry protocol.

## Isolation and fairness

A busy actor mailbox does not require serializing unrelated actors. Eligible work
uses the existing PostgreSQL row locks, skip-locked selection, and actor ordering
checks. Multiple slots can skip busy actors and execute other eligible jobs.

The scheduler prefers eligible jobs in enqueue order. It does not implement
round-robin actor quotas, weighted tenant fairness, or a latency SLA. Tested mixed
loads demonstrate progress despite a long actor queue; they do not prove absence
of starvation for every sustained/adversarial workload. Slot count also does not
guarantee balanced placement between nodes.

Concurrency increases memory and IO demand. Wasm instances remain isolated with
per-invocation limits, while the compiled component cache is shared. The current
PostgreSQL adapter has four connections per adapter, so raising slots beyond four
does not increase its simultaneous database connection count. Storage, CPU,
compilation, or memory may become the limiting resource.

## Verification

The focused tests use a gated guest engine to hold the first wave of actors until
all configured slots are occupied. This establishes actual overlap independently
of wall-clock speed. They check one node with four slots and two nodes with four
slots each. Each scenario submits:

- One hot actor with 128 ordered messages.
- Sixteen other actors with eight ordered messages each.
- Thirty-two ordinary tasks queued behind those messages.

Assertions cover exact actor state sequences and receipts, no same-actor reentry,
exact task outputs, one task attempt, peak concurrency equal to configured
capacity, and ordinary task progress before the hot mailbox is exhausted.

Additional tests keep in-flight actors blocked through graceful shutdown for
longer than a lease, then check successful commit and that a late message remains
queued. Cancellation tests check that both slots and heartbeat disappear. Invalid
slot counts fail before registering a node.

The same mixed-load scenario runs against PostgreSQL. A separate Compose harness
uses real Wasm research actors on two four-slot worker containers, checks state
isolation and duplicate approvals, and removes only its isolated project afterward:

```sh
scripts/local-env.sh docker compose --profile tests build node-a tests
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/multi_actor_acceptance.py
```

These are concurrency and correctness checks, not a throughput benchmark.

## Recorded results

All checks below passed on this revision:

| Check | Result |
| --- | --- |
| Workspace compilation, formatting, Clippy with warnings denied | PASS |
| Host workspace tests | 28 behavior tests and one generated doctest; seven integration tests deferred to the container run |
| Linux workspace plus all explicit PostgreSQL/Wasm integrations | 35 behavior/integration tests and one generated doctest |
| Gated mixed workload, memory and PostgreSQL | One-node/four-slot and two-node/eight-slot overlap, exact state/order, no actor reentry, bounded peak concurrency, and task progress despite the hot mailbox |
| Real Wasm Compose scenario | 16 actors, 128 analysis tasks, isolated results, idempotent duplicate approvals, state retained after both four-slot workers stop |
| Existing Compose fault/recovery suite | All six groups, using the default one-slot configuration, including worker SIGKILL, committed-work preservation, actor recovery, database-volume persistence, and Wasm trap isolation |

The local development workers were also restarted with four slots each. No
throughput comparison or general fairness guarantee is inferred from these runs.
