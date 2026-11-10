# Partitioned shuffle

Loom implements storage-mediated hash shuffle using Task, ObjectRef, and the
existing execution/lease contracts. Shuffle is a task input/output mode, not a
separate Flow primitive. It contains no word-count-specific logic.

## Data path

```text
input object A -> map task A -> A0 A1 A2 partition blocks -> manifest A
input object B -> map task B -> B0 B1 B2 partition blocks -> manifest B

reducer 0 reads A0 B0 -> output partition 0
reducer 1 reads A1 B1 -> output partition 1
reducer 2 reads A2 B2 -> output partition 2
```

Each mapper reads only its assigned input. The native host partitions that mapper's
output records and publishes immutable blocks through `ObjectStore`. A reducer's host
reads manifest metadata from all its sources, but fetches data blocks only for its
own partition. It folds one block at a time through Wasm. Drivers submit task
specifications and track outcomes; intermediate data never needs to pass through
the driver.

There is no peer-to-peer transfer or distributed object store implementation yet.
With the PostgreSQL adapter, data passes through `loom_blobs`. Compute and block
production/consumption are distributed; the database remains a central storage and
coordination bottleneck.

## Contracts

Types live in [`loom-contracts`](../crates/contracts/src/lib.rs).

```rust
let layout = ShuffleLayout { partitions: 4 };
// Mapper TaskSpec:
output: TaskOutput::Shuffle(layout.clone()),

// Reducer TaskSpec:
input: Input::Shuffle {
    sources: mapper_ids.into_iter().map(ShuffleSource::TaskResult).collect(),
    layout,
    partition: 0,
},
output: TaskOutput::Value,
```

A mapper returns a JSON array of `ShuffleRecord { key: Vec<u8>, value: Vec<u8> }`.
The key and value codecs belong to the application. `loom_guest_sdk::record(key,
&value)` JSON-encodes a typed value into those opaque value bytes. The host routes
records without interpreting their values or aggregating keys.

`ShuffleSource::TaskResult(id)` waits for a committed mapper result. Sources must
already exist when the reducer is submitted, which prevents forward references
and cycles. Duplicate task IDs, incompatible mapper layouts, invalid partition
indices, and excessive source counts are rejected before enqueueing.

`ShuffleSource::Manifest(reference)` consumes an existing immutable shuffle
manifest directly. This can reuse a previous stage's output. Repeated manifest
references intentionally preserve multiplicity: two distinct map tasks can produce
identical content and must both contribute to a result.

## Versioned partitioning and storage format

Version 1 uses exactly:

```text
partition = big_endian_u64(SHA256(key_bytes)[0:8]) % partition_count
```

This does not depend on Rust's randomized hashers, machine endianness, or process
identity. Equal key bytes always route together. Applications must choose a stable
key encoding; string keys in Word Count are unnormalized UTF-8 bytes.

The mapper's committed task result references a `ShuffleManifest`:

```text
version: 1
layout: { partitions: P }
partitions: [
  [ { object: ObjectRef, records: count, bytes: encoded_length }, ... ],
  ... P entries ...
]
```

Each block is a JSON array of opaque records, at most 64 KiB encoded. A large
partition spans multiple blocks. Empty partitions have no blocks. One encoded
record must fit within a block. Records retain mapper emission order within a
partition; shuffle does not sort keys or promise a global ordering.

Only block references, record counts, and byte lengths live in the manifest. All
blocks are written before the manifest, and the task's existing fenced commit
publishes the manifest reference. Reducers check manifest version/layout, block
lengths, record counts, and routing of every selected record.

The implementation is [`loom-shuffle`](../crates/shuffle/src/lib.rs). It depends on
contracts and serialization/hash libraries, not PostgreSQL, Wasmtime, or a driver.

## Dependency scheduling

The execution backend checks all task sources in one claim transaction:

- If any source has failed, the reducer becomes failed without executing guest code.
- If any source is unfinished, the reducer remains pending and consumes no attempt.
- Once every source has succeeded, task references resolve to committed manifest
  references in the claimed input. The task's stored specification remains unchanged.

The reference scheduler waits for all specified mappers, including ones that will
produce an empty block list for the selected partition. There is no early streaming
of uncommitted map output. Nodes claim eligible work; a partition is not permanently
assigned to a particular node.

## Guest reducer interface

The guest implements a fold over a block of records:

```rust
pub trait Reducer {
    type State: Default + Serialize + DeserializeOwned;
    fn fold(state: Self::State, records: Vec<ShuffleRecord>) -> Result<Self::State, String>;
}
```

The component dispatches its reduce function to `run_reducer::<MyReducer>(&input)`.
The runtime sends `ReduceFrame { state, records }` through the existing task ABI.
For the first block, `state` is absent and the SDK uses `State::default()`. Subsequent
blocks receive the previous encoded state. A fresh Wasm instance runs each fold;
state survives between folds only because the host passes it explicitly. An empty
partition gets one empty fold call to produce a valid initial result.

Only the final fold output is committed as the reducer's result. Reducers should
have no external effects. This is incremental block consumption, not durable
checkpointing of each fold. If a reducer fails, it retries the whole partition
from immutable source blocks and a fresh accumulator.

A reducer may also declare shuffle output if its final value is a record array,
allowing another shuffle stage using the same contracts.

## Failure semantics

A map failure during block publication may leave unreferenced immutable objects.
They do not enter the data path because no successful task outcome references a
partial manifest. Retrying can reuse identical content-addressed objects.

A stale mapper or reducer cannot commit after its lease/epoch is replaced. An exact
retry of the same successful commit returns the original receipt. A replacement
attempt can publish a new manifest, but downstream tasks see only the winning
committed reference. Failed dependencies propagate through the existing claim path.

Two maps with identical manifests are still two inputs. Removing duplicate digests
would lose data; only duplicate logical task IDs in a submitted source list are
rejected.

## Limits and upgrades

This is a bounded first implementation:

- 1–256 partitions and at most 1,024 sources per reducer.
- At most 4,096 blocks per manifest; block payloads are at most 64 KiB.
- A mapper's complete emitted record array still fits within the existing 4 MiB
  Wasm output limit and is materialized before partitioning.
- A reducer keeps one fetched block plus its accumulator between guest calls. Each
  serialized frame and result must fit the 4 MiB payload limit. Because accumulator
  bytes are embedded in JSON, the usable state size can be smaller. Wasm memory and
  fuel limits still apply per invocation.
- There is no spill-to-disk accumulator, key sorting, automatic skew handling,
  backpressure-aware producer streaming, or object garbage collection yet.
- Backend metadata updates still serialize on one namespace ledger row.

A hot key can dominate a partition. More partitions do not split one key; skew
requires application-side combining/salting or a later multi-stage strategy.

Old task records deserialize with `output = Value`; existing Wasm task/actor entry
points are unchanged. New `Input::Shuffle` records require upgraded workers.
Upgrade **all workers sharing a namespace** before submitting shuffle tasks;
rolling mixed-version execution is not supported by this prototype.

The [Word Count example](quickstart-word-count.md) uses this protocol. The original
sorting example retains its local merge and ordinary value outputs.

See [verification coverage](shuffle-verification.md) for tested behaviors and limits.
