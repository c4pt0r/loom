# Backend and execution contracts

## Atomic boundaries

An adapter must serialize ownership validation and state commit together. Checking
a lease with one service and later writing state through another is insufficient.
Loom's `ExecutionStore` owns claim, renewal and commit for that reason.
`Membership` and `ExecutionStore` share authority in the reference implementations.

`create_actor`, `send`, and `submit` are idempotent by ID and exact request content.
The actor definition (including immutable module digest) cannot silently change.
IDs are namespace-local, 1–512 bytes. Persisted records use ledger schema version 1.
Guest ABI version is `loom:guest@0.1.0`; new ABI/schema versions require explicit migration.

For actor turns, commit includes:

1. Validate live node incarnation and work lease epoch.
2. Compare actor revision.
3. Insert task intents with deterministic IDs derived from message and intent index.
4. Insert outgoing/delayed messages with deterministic IDs.
5. Write state, increment revision, and record reply and message receipt.

Any error rolls back every operation. Adapters must not leak early writes if a
later intent fails validation. In memory this is clone-and-swap under a mutex;
PostgreSQL uses a locked row and a transaction. A future adapter may use different
tables or a replicated state machine while preserving observable semantics.

For task completion, immutable result publication precedes the transaction. The
transaction stores success and its actor notification together. A failed commit
may leave an unreferenced object, which is safe to retain; a future GC must account
for in-progress publication. The caller must publish valid blobs before committing
their references. All current objects are retained indefinitely.

## Failure and retry

`Unavailable` from a mutation may mean the operation committed but the reply was
lost. Query `receipt(job_id)` or retry the exact request. Never invent a new ID.
The runtime does not mark work failed merely because commit acknowledgement failed.

Task and actor attempt counts include the initial execution. Losing a worker
consumes that attempt. Reclaiming increments the epoch. Node expiry fences claims
even if the individual work deadline has not yet elapsed. Renewing a node does not
resurrect an expired claim; renewing a claim requires a live node.

Actor turns serialize across nodes. Future timers do not block ready messages,
but a currently executing turn does, including when an earlier timer becomes due.
Failed turns block later ready messages while retrying. After the finite retry
budget is exhausted, the message is quarantined as a failed receipt; state stays
at the previous revision. Queued messages remain available for processing.

No external exactly-once guarantee is made. Future HTTP/model/tool capabilities
must propagate stable operation IDs, support idempotency where available, and
represent uncertain outcomes explicitly. These capabilities are out of this slice.

## Membership

Each boot supplies a fresh random incarnation. A live registration rejects a
different incarnation for the same node ID. Expired sessions cannot renew or drain
replacement sessions. TTL is evaluated using the storage authority's clock after
serialization locks are acquired. A draining node receives no new claims.
Registration records are not a substitute for work ownership.

## API scope

Rust guests export typed Task and Actor logic through a WIT component boundary.
The initial `Decision` ABI encodes state, replies and intents as JSON payloads.
`ObjectRef<T>` supplies compile-time intent; it is not a runtime schema proof or a
capability token. The prototype assumes trusted host callers and controlled modules.

Deferred: ActorRef/ask client ergonomics, cancellation, hierarchical supervision,
ephemeral actors, module hot upgrade, stream/channel primitives, component capability
imports, storage sharding, object GC, authentication and TLS.
