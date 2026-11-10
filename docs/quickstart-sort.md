# Quickstart: distributed sorting with Rust and Loom

Split an array of integers into chunks, sort the chunks on two Loom workers, and
merge their results into one sorted array. Both the task and the client are written
in Rust. You will use **Task** to run computation and **ObjectRef** to pass immutable
inputs and results.

This guide first runs the example, then walks through its implementation. The
sorting code compiles to a Wasm Component. Rust worker processes claim tasks,
execute the component, and commit results. Docker builds everything, so you only
need Docker Engine and Compose on your machine.

## 1. Start two workers

Clone the repository, or enter its root if you already have the source:

```sh
git clone https://github.com/c4pt0r/loom.git
cd loom
```

Define a shortcut for Compose. The wrapper supports the development workspace's
local Colima instance and otherwise uses your existing Docker configuration:

```sh
dc() { scripts/local-env.sh docker compose "$@"; }

dc --profile tools build node-a
dc up -d --wait --no-recreate postgres node-a node-b
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" members
```

Compose configures `LOOM_DATABASE_URL` as a PostgreSQL URI DSN:
`postgresql://loom:loom-local-only@postgres:5432/loom`. Native clients and workers
use the same URI format, with a hostname they can reach. Percent-encode reserved
characters in usernames and passwords, for example `@` as `%40`.

The first build downloads dependencies and compiles Rust and Wasmtime; allow a few
minutes. `--no-recreate` preserves workers that are already running. They can load
the new sorting component without restarting. `members` should list `node-a` and
`node-b`; if registration is still in progress, run it again shortly.

## 2. Run distributed sorting

```sh
dc --profile tools run --rm -T sort
```

The client generates **16,000 integers** and submits **16 tasks**, each sorting
1,000 integers. It uploads the Wasm component automatically, submits all chunks,
and waits for their results. You will first see:

```text
Submitted 16 tasks (run: sort-...)
```

The JSON report contains:

| Field | Meaning |
| --- | --- |
| `count: 16000` | Number of sorted integers |
| `task_count: 16` | Number of submitted tasks |
| `verified: true` | Complete result matches a local reference sort, including duplicates |
| `output` | ObjectRef digest of the complete sorted array |
| `tasks` | Task IDs, attempt counts, and output digests for individual chunks |
| `run` | Unique identifier for finding this run in worker logs |

The local reference sort checks correctness. A failed remote task causes the
client to return an error.

To sort your own data, pass a JSON array of signed 64-bit integers:

```sh
printf '%s\n' '[9, -2, 7, 7, 0, 3, -8, 1]' |
  dc --profile tools run --rm -T sort --stdin --chunk-size 3 --print-result
```

This submits three tasks. The report includes:

```json
"sorted": [-8, -2, 0, 1, 3, 7, 7, 9]
```

You can also read from a file containing a JSON array:

```sh
dc --profile tools run --rm -T sort --stdin --print-result < numbers.json
```

Omit `--print-result` to avoid printing a large array. Retrieve the saved result
later using the report's `output` digest:

```sh
# Replace OUTPUT_DIGEST with the report's output value.
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" get OUTPUT_DIGEST
```

Each run gets new task IDs, so you can repeat these commands without conflicting
with previous runs.

## 3. Understand the execution model

```text
Rust client
  split input -> store chunk objects -> submit all sort_chunk tasks
                                          |
                          +---------------+---------------+
                          |                               |
                     node-a: Wasm                    node-b: Wasm
                          |                               |
                          +------ commit result refs -----+
                                          |
Rust client
  wait for tasks -> read sorted chunks -> k-way merge -> store final object
```

The client submits work through Loom's backend interfaces. Workers poll the shared
queue and claim available tasks. The client does not select a worker or establish
a direct connection to it. Distribution depends on availability; there is no
promise that each worker receives half the chunks.

The implementation lives in two small packages:

| Source | Runs where | Responsibility |
| --- | --- | --- |
| [`examples/sort-guest/src/lib.rs`](../examples/sort-guest/src/lib.rs) | Wasm inside a worker | Sort one chunk |
| [`examples/sort-client/src/main.rs`](../examples/sort-client/src/main.rs) | Native Rust client | Submit tasks, collect results, and merge |

The following excerpts use the implemented API. Read the linked files for imports,
argument parsing, and the complete executable.

## 4. Define a typed Rust task

The guest's computation is an ordinary Rust implementation of `Task`:

```rust
struct SortChunk;

impl Task for SortChunk {
    type Input = Vec<i64>;
    type Output = Vec<i64>;

    fn run(mut numbers: Vec<i64>) -> Result<Vec<i64>, String> {
        numbers.sort_unstable();
        Ok(numbers)
    }
}
```

`Input` and `Output` define the types at the task boundary. The guest SDK decodes
incoming JSON into `Vec<i64>`, calls `run`, and serializes the returned vector.
`sort_unstable` preserves duplicate values; stability is unnecessary when sorting
plain integers.

The task has no database connection or node-selection logic. It computes solely
from its input and has no external side effects, so retrying it is safe.

### Export the task as a Wasm Component

The worker invokes a named function through the component ABI. The guest routes
`sort_chunk` to the typed task:

```rust
struct SortGuest;

impl Guest for SortGuest {
    fn task(name: String, input: Vec<u8>) -> Result<Vec<u8>, String> {
        match name.as_str() {
            "sort_chunk" => run_task::<SortChunk>(&input),
            _ => Err(format!("unknown task: {name}")),
        }
    }

    fn handle(_: String, _: Vec<u8>, _: Vec<u8>) -> Result<Vec<u8>, String> {
        Err("this component only exports tasks".into())
    }
}

export!(SortGuest with_types_in loom_guest_sdk::bindings);
```

`Guest::task` is the byte-oriented entry point. `run_task::<SortChunk>` bridges it
to the typed Rust implementation. `export!` exposes the WIT interface. The shared
interface also has an actor entry point, `handle`; this task-only component returns
an error if that entry point is called.

The Dockerfile compiles this crate for `wasm32-unknown-unknown`, then packages it
with `loom component`. Workers load the resulting component by its content digest.

## 5. Submit all chunks before waiting

The client saves the component and receives an immutable module reference:

```rust
let module = store.put(std::fs::read(&args.component)?).await?;
```

The sorting function accepts `&dyn Backend`. For each input chunk, it stores the
serialized integers and submits a task referring to that object:

```rust
for (index, chunk) in input.chunks(chunk_size).enumerate() {
    let object = store.put(serde_json::to_vec(chunk)?).await?;
    let id = format!("{run}-{index}");
    store
        .submit(TaskSpec {
            id: id.clone(),
            module: module.clone(),
            function: "sort_chunk".into(),
            input: Input::Object(object),
            output: TaskOutput::Value,
            max_attempts: 3,
            notify: None,
        })
        .await?;
    ids.push(id);
}
```

Each field controls a concrete part of execution:

| Field | Purpose |
| --- | --- |
| `id` | Stable identity for this logical task across execution attempts |
| `module` | Reference to the uploaded sorting Wasm Component |
| `function` | Name matched by `Guest::task` |
| `Input::Object(object)` | Reference to the serialized chunk; the worker resolves it before calling the guest |
| `max_attempts: 3` | Allow up to three attempts in total, including the first |
| `notify: None` | No actor completion notification; this client polls task status |

An `ObjectRef` identifies immutable stored bytes. It does not represent an
in-progress computation. The task ID is what the client uses to track execution;
a successful task eventually produces an output `ObjectRef`.

**Submit every chunk before waiting for completion.** Each `submit().await` waits
for the submission to be recorded, not for sorting to finish. Workers can execute
already-submitted chunks while the client submits the rest. Waiting for a result
inside this loop would serialize the workload.

## 6. Collect results and handle failures

The client polls outstanding task IDs. This is the result-handling branch inside
its polling loop:

```rust
let status = store.task(&id).await?;
match status.outcome {
    Some(TaskOutcome::Succeeded(object)) => {
        let chunk: Vec<i64> =
            serde_json::from_slice(&store.get(&object).await?)?;
        ensure!(
            chunk.windows(2).all(|w| w[0] <= w[1]),
            "unsorted shard: {id}"
        );
        completed.push(serde_json::json!({
            "id": id,
            "attempts": status.attempts,
            "output": object.digest,
        }));
        chunks.push(chunk);
    }
    Some(TaskOutcome::Failed(error)) => bail!("task {id} failed: {error}"),
    None => pending.push(id),
}
```

`None` means no terminal outcome is available. The client keeps that ID in the
pending set and waits 100 ms between polling rounds. A success returns a reference;
`store.get` fetches its bytes, and the client decodes the sorted chunk. A terminal
failure stops the operation with the task ID and error.

The client wraps submission and collection in a 120-second timeout, configurable
with `--timeout-seconds 300`. A timeout ends the client's wait; it does not cancel
submitted tasks. If a worker disappears before committing a result, the task can
be claimed again after its lease expires, within its attempt budget. Committed
results remain in the backend. This example client does not implement resumable
orchestration after a client crash.

## 7. Merge the sorted chunks

The client performs a k-way merge using a min-heap. Rust's `BinaryHeap` is a
max-heap, so wrapping each tuple in `Reverse` makes the smallest value come first:

```rust
fn merge(chunks: Vec<Vec<i64>>) -> Vec<i64> {
    let mut heap = BinaryHeap::new();
    let mut output = Vec::with_capacity(chunks.iter().map(Vec::len).sum());
    for (shard, chunk) in chunks.iter().enumerate() {
        if let Some(&value) = chunk.first() {
            heap.push(Reverse((value, shard, 0)));
        }
    }
    while let Some(Reverse((value, shard, index))) = heap.pop() {
        output.push(value);
        if let Some(&next) = chunks[shard].get(index + 1) {
            heap.push(Reverse((next, shard, index + 1)));
        }
    }
    output
}
```

The heap holds the next available element from each nonempty chunk. After taking
the smallest element, the algorithm advances only that element's chunk. Chunk
completion order does not matter. Duplicates remain separate elements, and empty
input yields an empty result.

For `N` integers across `K` chunks, merging costs `O(N log K)` time and uses up to
`O(K)` heap entries. The example keeps all chunk data and the output in client
memory, so total client memory is `O(N)`.

Finally, the client compares the merged array with a local reference sort and
stores the verified array with `store.put`, producing the report's final `output`
reference. The reference sort is part of this tutorial's validation and adds local
work; it is not a distributed performance benchmark.

### Keep application logic independent of PostgreSQL

The sorting function's storage dependency is an interface:

```rust
async fn distributed_sort(
    store: &dyn Backend,
    module: ObjectRef,
    input: &[i64],
    chunk_size: usize,
    run: &str,
) -> Result<(Vec<i64>, Vec<serde_json::Value>)>
```

`Backend` combines the execution, object-storage, and membership contracts. Only
`main` selects the concrete adapter:

```rust
let postgres = Arc::new(
    loom_backend_postgres::connect(&args.database_url, &args.namespace).await?,
);
let store = Stores::new(postgres.clone(), postgres);
```

Replacing that adapter does not require changing the sorting algorithm. Clients
and workers must still use a compatible backend and the same namespace. The guest
has no storage dependency at all. This batch computation uses tasks and object
references; it needs no persistent actor state.

## 8. Inspect execution and modify the example

Use the report's full `run` value to find this run in worker logs:

```sh
# Replace RUN_ID with the full value, including the sort- prefix.
dc logs --no-color node-a node-b | grep 'RUN_ID'
```

Container prefixes identify the workers. `COMMIT task:sort-...` means a task result
was committed. The default 16-task example usually uses both workers; a small input
may be handled entirely by one.

If the client keeps waiting, inspect workers and an individual task:

```sh
dc ps
dc logs --tail 30 node-a node-b
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" task TASK_ID
```

After editing the Rust example, rebuild and run it again:

```sh
dc --profile tools build node-a
dc --profile tools run --rm -T sort
```

The new image contains the updated client and component. Existing workers load
the component by its new digest, so changes to the example do not require worker
restarts. If you change the host runtime, stop the workers and let their five-second
registration leases expire before replacing them:

```sh
dc stop node-a node-b
sleep 6
dc up -d --wait node-a node-b
```

## Scope and verification

**Chunk sorting runs on distributed workers; final merging runs in client memory.**
The client retains the input, sorted chunks, output, and a verification copy. This
example is intended for learning task execution and result handling, not for sorting
files larger than client memory.

Chunk size is limited to 1–1,000 integers to fit the current Wasm fuel and payload
limits. Inputs, outputs, and task records remain in PostgreSQL; automatic cleanup
is not implemented. The reference backend serializes ledger updates within a
namespace, so this example makes no claim of speedup over local sorting.

Stop the environment while retaining its database volume:

```sh
dc down
```

Run the external acceptance harness:

```sh
scripts/local-env.sh python3 scripts/sort_acceptance.py
```

It compares complete results with Python's `sorted` for the default input, custom
input, an empty array, duplicates, and integer boundaries. It also checks that both
workers committed tasks from the default run and that invalid input is rejected.
These checks passed in the local two-worker Compose environment; see the
[verification record](sort-verification.md).
