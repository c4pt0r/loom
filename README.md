# Loom

Rust applications compiled to WebAssembly Components, executed by a Rust host.

This is the first executable prototype: Task, durable Actor, ObjectRef, and a
minimal OneForOne restart policy. Flow is deliberately absent. Channel is deferred.

The examples cover distributed sorting, word count, and a durable agent state
machine that fans out analysis, aggregates results, waits for approval, and resumes
after restart.

Examples: [Sorting](#quickstart-distributed-sorting-with-rust-and-loom) ·
[Word count](#quickstart-distributed-word-count) · [Shuffle protocol](docs/shuffle.md) · [Object storage](docs/object-store.md) · [Coordination and input slices](docs/scaling-and-inputs.md)

## Quickstart: distributed sorting with Rust and Loom

Split an array of integers into chunks, sort the chunks on two Loom workers, and
merge their results into one sorted array. Both the task and the client are written
in Rust. You will use **Task** to run computation and **ObjectRef** to pass immutable
inputs and results.

This guide first runs the example, then walks through its implementation. The
sorting code compiles to a Wasm Component. Rust worker processes claim tasks,
execute the component, and commit results. Docker builds everything, so you only
need Docker Engine and Compose on your machine.

### 1. Start two workers

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

### 2. Run distributed sorting

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

### 3. Understand the execution model

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
| [`examples/sort-guest/src/lib.rs`](examples/sort-guest/src/lib.rs) | Wasm inside a worker | Sort one chunk |
| [`examples/sort-client/src/main.rs`](examples/sort-client/src/main.rs) | Native Rust client | Submit tasks, collect results, and merge |

The following excerpts use the implemented API. Read the linked files for imports,
argument parsing, and the complete executable.

### 4. Define a typed Rust task

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

#### Export the task as a Wasm Component

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

### 5. Submit all chunks before waiting

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

### 6. Collect results and handle failures

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

### 7. Merge the sorted chunks

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

#### Keep application logic independent of PostgreSQL

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

### 8. Inspect execution and modify the example

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

### Scope and verification

**Chunk sorting runs on distributed workers; final merging runs in client memory.**
The client retains the input, sorted chunks, output, and a verification copy. This
example is intended for learning task execution and result handling, not for sorting
files larger than client memory.

Chunk size is limited to 1–1,000 integers to fit the current Wasm fuel and payload
limits. Inputs, outputs, and task records remain in PostgreSQL; automatic cleanup
is not implemented. PostgreSQL remains shared storage; this example makes no
claim of speedup over local sorting.

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
[verification record](docs/sort-verification.md).

## Quickstart: distributed word count

Run Map, shuffle, and Reduce on Loom workers. Each mapper counts its own text
chunk, the host partitions records by key, and reducers independently add the
counts for their partitions. The driver submits the graph and returns references
to the final partitions; it does not merge intermediate results.

### Run the example

From the repository root, build and start the environment. If upgrading from the
older example, stop both workers and let their registration leases expire before
starting the new host binaries:

```sh
dc() { scripts/local-env.sh docker compose "$@"; }
dc --profile tests build node-a tests
dc stop node-a node-b
sleep 6
dc up -d --wait postgres node-a node-b
dc --profile tools run --rm -T word-count --reducers 4
```

Do not run old workers alongside new ones in the same namespace: old hosts cannot
interpret shuffle inputs. PostgreSQL data is retained through the upgrade. Compose
uses the standard DSN `postgresql://loom:loom-local-only@postgres:5432/loom`.

The default input repeats `hello loom hello wasm` 4,000 times. The program submits
**16 Map tasks and 4 Reduce tasks**. Its JSON report includes:

| Field | Meaning |
| --- | --- |
| `total_words` | Input token count: 16,000 |
| `map_task_count` / `reduce_task_count` | 16 / 4 |
| `output` | ObjectRef digest of the final partitioned-dataset manifest |
| `partitions` | Final reducer output references, in partition-index order |
| `map_tasks` / `reduce_tasks` | Task IDs, attempts, and committed output digests |
| `run` | Unique run ID for logs |

The default driver fetches no intermediate or final data blocks. To display the
completed counts, explicitly request the final partitions:

```sh
dc --profile tools run --rm -T word-count --reducers 4 --print-result
```

The report then also includes `counts` and `unique_words`:

```json
{"hello": 8000, "loom": 4000, "wasm": 4000}
```

`--print-result` concatenates already-reduced, disjoint key maps. It does not sum
Map results locally and rejects duplicate keys across final partitions.

Supply your own plain UTF-8 input:

```sh
printf 'hello loom hello\nwasm loom\n' |
  dc --profile tools run --rm -T word-count --stdin --chunk-words 2 --reducers 4 --print-result

# Or read a text file:
dc --profile tools run --rm -T word-count --stdin --reducers 4 < book.txt
```

The small example creates three Maps and four Reducers and returns
`{"hello": 2, "loom": 2, "wasm": 1}`. Every run has new task IDs.

### Who reads which data?

```text
Driver: split text and submit tasks

input chunk A -> Mapper A -> A0 A1 A2 A3
input chunk B -> Mapper B -> B0 B1 B2 B3
                              |
                       shared object storage
                              |
        +-------------+-------+-------+-------------+
        |             |               |             |
    Reducer 0     Reducer 1       Reducer 2     Reducer 3
    reads A0 B0   reads A1 B1     reads A2 B2   reads A3 B3
        |             |               |             |
     output 0      output 1        output 2      output 3
```

The driver decides the input-to-task mapping. Nodes atomically claim eligible
tasks, so any worker may act as a mapper or reducer. Each mapper reads only its
assigned input object. Each reducer reads manifests from all mappers, but data
blocks only for its own partition. No mapper scans the entire original input.

The sample and stdin modes still read the complete text and split it locally.
For existing objects, `--input-slices slices.json` submits byte-range references
and `--input-object DIGEST` submits a complete object without driver-side source
reads. Existing complete-object inputs remain supported. See
[object slices and encoding boundaries](docs/scaling-and-inputs.md).

### Map: emit typed values under byte keys

The guest code is in
[`examples/word-count-tasks/src/lib.rs`](examples/word-count-tasks/src/lib.rs).
It first combines repeated words within one chunk, then emits records:

```rust
impl Task for CountWords {
    type Input = String;
    type Output = Vec<ShuffleRecord>;

    fn run(text: String) -> Result<Self::Output, String> {
        let mut counts = BTreeMap::<String, u64>::new();
        for word in text.split_whitespace() {
            *counts.entry(word.to_owned()).or_insert(0) += 1;
        }
        counts.into_iter()
            .map(|(key, value)| record(key.as_bytes(), &value))
            .collect()
    }
}
```

`record` preserves the UTF-8 key bytes and serializes the `u64` value. The generic
shuffle layer does not know these records represent words or counts.

The driver explicitly selects `ShuffleLayout::hash(args.reducers)` so that equal
words reach the same reducer. General shuffle defaults to random redistribution
with `ShuffleLayout::new(partitions)`.

The driver declares `output: TaskOutput::Shuffle(layout.clone())` on each Map
`TaskSpec`. That tells the host to partition the returned records using
`big_endian_u64(SHA256(key)[0:8]) % partition_count`, publish immutable blocks, and
commit a manifest reference. It replaces the ordinary single-value output mode.

The Rust guest is compiled to Wasm during image build and uploaded at runtime.
Wasm functions are dispatched by `Guest::task`: `count_words` calls
`run_task::<CountWords>`, and `sum_counts` calls `run_reducer::<SumCounts>`.

### Submit reducers before the maps finish

The Rust entry point is
[`examples/word-count/src/main.rs`](examples/word-count/src/main.rs). After
submitting all Maps, it submits one reducer per partition:

```rust
let sources = map_ids.iter().cloned()
    .map(ShuffleSource::TaskResult).collect::<Vec<_>>();

store.submit(TaskSpec {
    id,
    module: module.clone(),
    function: "sum_counts".into(),
    input: Input::Shuffle {
        sources: sources.clone(),
        layout: layout.clone(),
        partition,
    },
    output: TaskOutput::Value,
    max_attempts: 3,
    notify: None,
}).await?;
```

The execution backend waits until every Map has committed its manifest. A pending
reducer consumes no attempt. If any Map fails terminally, its dependent reducers
fail without running. Once ready, the claim resolves upstream task IDs to immutable
manifest references. The worker then fetches its selected partition blocks.

The application's orchestration function depends on `&dyn Backend`; PostgreSQL
is selected only in `main`. Task and storage interfaces remain implementation-independent.

### Reduce: fold blocks on a worker

The guest defines the accumulator and how to consume one block:

```rust
impl Reducer for SumCounts {
    type State = BTreeMap<String, u64>;

    fn fold(mut state: Self::State, records: Vec<ShuffleRecord>)
        -> Result<Self::State, String>
    {
        for record in records {
            let word = String::from_utf8(record.key).map_err(|e| e.to_string())?;
            let count: u64 = decode(&record.value)?;
            let total = state.entry(word).or_default();
            *total = total.checked_add(count).ok_or("word count overflow")?;
        }
        Ok(state)
    }
}
```

The host invokes this fold once per block, passing the previous encoded state into
the next call. It never fetches another partition's data. An empty partition gets
one empty fold call and produces `{}`. Only the final state is committed. A retry
starts the whole partition from an empty accumulator, so partially accumulated
counts are not counted twice.

No driver process holds the reduction state. All reducers can execute independently
once their inputs are ready, and task leases permit takeover on another worker.

### Read the final dataset

The `output` digest now points to a `PartitionedDataset`, not one global word map:

```text
{ version: 1, layout: { partitions: 4, strategy: "Hash" }, partitions: [ObjectRef, ...] }
```

Fetch the manifest, then any final partition you need:

```sh
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" get OUTPUT_DIGEST
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" get PARTITION_DIGEST
```

The driver publishes this small dataset manifest after all reducers succeed. It
has no resumable submission workflow; if it exits, already-submitted tasks can
continue and remain queryable, but you may need to recover their IDs to assemble
the final dataset manifest.

### Rules, limits, and verification

Words are separated by Unicode whitespace. Matching is case-sensitive, punctuation
is retained, and Unicode text is not normalized or language-segmented. `Loom`,
`loom`, and `loom,` are distinct keys. Empty input submits no Maps, but still runs
one empty reducer per requested partition.

The example accepts 1–1,000 words per chunk, a 16 KiB text chunk limit, at most
1,024 chunks, and 1–256 reducers. Encoded records must also fit a 64 KiB shuffle
block. The reducer accumulator and each serialized invocation must fit the
runtime's payload/memory limits. There is no spill-to-disk or automatic hot-key
splitting. PostgreSQL remains the shared storage bottleneck.

The default client timeout is 120 seconds; change it with `--timeout-seconds`.
Timeout does not cancel submitted tasks. Inputs, blocks, and results remain stored;
automatic garbage collection is not implemented.

Inspect execution or run independent verification:

```sh
# Replace RUN_ID with the complete word-count-... identifier.
dc logs --no-color node-a node-b | grep 'RUN_ID'
scripts/local-env.sh python3 scripts/word_count_acceptance.py
```

The harness checks final frequency maps, per-key partition ownership, actual Map
block hashes, persisted partitioned results, and both Map and Reduce execution on
two nodes. See the [shuffle protocol](docs/shuffle.md) and
[verification record](docs/word-count-verification.md) for semantics and coverage.

## Docker Compose

Docker Engine and Compose are sufficient; host Rust and PostgreSQL are not required.
The Dockerfile builds Linux binaries and the Rust Wasm component from source.

```sh
docker compose --profile tests build node-a tests
docker compose up -d --wait postgres node-a node-b
docker compose --profile tools run --rm cli --namespace lab members

# Rust contracts, PostgreSQL/Wasm integration and local Wasm demo inside Linux:
docker compose --profile tests run --rm tests

# Actual container SIGKILL, worker recovery and persistent-volume acceptance:
python3 scripts/compose_acceptance.py --report docs/compose-acceptance.json
```

The acceptance harness creates a unique Compose project and namespace, then removes
only its own containers and volume. The normal `loom-local` development stack stays
running. Test helpers are behind profiles and are not started by ordinary `up`.

On the Mac used to build this project, Docker runs in a workspace-local Colima VM.
Prefix commands with `scripts/local-env.sh` to use that engine without changing
your global Docker context:

```sh
scripts/local-env.sh docker compose ps
scripts/local-env.sh docker compose --profile tests run --rm tests
scripts/local-env.sh python3 scripts/compose_acceptance.py --report docs/compose-acceptance.json
```

See [the Compose guide](docs/compose.md) for guest deployment, logs, persistence and
VM restart instructions. PostgreSQL is only exposed on the internal Compose network.

## Run locally

Requirements: Rust 1.93+ with Cargo; `wasm32-unknown-unknown` target. The initial
build downloads Wasmtime and the component toolchain from crates.io.

```sh
rustup target add wasm32-unknown-unknown
cargo build --locked -p loom-cli
target/debug/loom build
target/debug/loom demo target/research-agent.component.wasm
```

The final output should show `awaiting_approval`, then `done`, and `PASS`.
`demo` uses volatile memory storage. `loom build` currently builds the bundled Rust
example. Other guests can be compiled with Cargo and packaged with
`loom component input.wasm output.component.wasm`.

## Run two durable nodes

Use a development PostgreSQL database. The adapter creates node, actor, job,
dependency, object, and schema-metadata tables; namespaces isolate logical clusters. The prototype connects without
TLS and is intended for a trusted local environment.

```sh
export LOOM_DATABASE_URL='postgresql://YOUR_USER:YOUR_PASSWORD@127.0.0.1:5432/postgres'

target/debug/loom --namespace lab deploy target/research-agent.component.wasm
# Copy the returned module digest:
target/debug/loom --namespace lab create-actor research MODULE_DIGEST
```

Run each worker in a separate terminal with the same database configuration:

```sh
target/debug/loom --namespace lab node --id worker-a
target/debug/loom --namespace lab node --id worker-b
```

Send commands and inspect durable state:

```sh
target/debug/loom --namespace lab send research --id start \
  '{"start":["Rust Wasm actors","Durable agent execution"]}'
target/debug/loom --namespace lab actor research
# Once phase is awaiting_approval:
target/debug/loom --namespace lab send research --id approval '{"approve":true}'
target/debug/loom --namespace lab actor research
```

Repeating the same message ID and content is idempotent. Reusing the ID with
different content is rejected. Stop/restart workers while the actor waits: its
state and pending messages remain in PostgreSQL. Namespace IDs must be unique per
independent run, or use new actor/message/task IDs.

## Rust guest programming

`loom-guest-sdk` exposes typed `Task` and `Actor` traits. The example shows their
dispatch through two WIT exports; JSON is the initial payload encoding.

```rust
impl Task for Analyze {
    type Input = String;
    type Output = Analysis;

    fn run(input: String) -> Result<Analysis, String> {
        // Compute a result. No process-local state is retained between tasks.
    }
}
```

An actor returns a `Turn<State, Reply>` containing its new state, response, task
intents and outgoing messages. The host atomically commits the entire turn.
Task completion is delivered as a typed `TaskCompleted` message, including either
an immutable output reference or an error. Timers are delayed outgoing messages.
There are no suspended durable Rust futures and no Wasm heap snapshots.

See [the Rust guest](examples/research-agent/src/lib.rs),
[SDK](crates/guest-sdk/src/lib.rs), and [WIT boundary](wit/guest.wit).

## Object storage

Task coordination and payload storage are independently injectable. `ControlStore`
keeps task state, leases, fencing, and membership together. `ObjectStore` holds
Wasm components, task data, shuffle blocks, and results through `put` / `get`,
with `get_range` for input slices.

```rust
let backend = Stores::new(control, objects);
```

The CLI and examples currently pass the PostgreSQL adapter for both roles. Another
object provider can replace the second argument without changing task or shuffle
logic. `WasmEngine` needs only the object interface. Task objects must remain
readable through retries and downstream consumption; there is no automatic TTL or
cleanup yet. See [object-store contracts and assembly](docs/object-store.md).

## Architecture

```text
Rust guest (Task / Actor traits)
             │ WIT Component exports
             ▼
Wasmtime host ─── runtime orchestration
                         │
             implementation-independent contracts
                         │
        memory adapter / PostgreSQL reference adapter
```

- `contracts`: execution, membership, object storage, guest-engine interfaces, and `Stores` composition.
- `object-store-memory`: independent object-only adapter for tests and single-process development.
- `runtime`: claim → execute → publish object → commit; block-by-block shuffle reduction. No DB or Wasmtime dependency.
- `shuffle`: random redistribution by default, explicit key hashing, immutable blocks, and manifests.
- `wasm-host`: component cache; fresh isolated instance per turn; fuel and memory limits.
- `backend-core`: reference transition implementation used only by the adapters.
- `backend-memory`: volatile storage and injectable clock.
- `backend-postgres`: durable storage with database-authoritative lease time.
- `guest-sdk`: Rust task/actor traits and component bindings.
- `cli`: dependency injection and development commands.
- `conformance`: black-box behavioral tests using public contracts.

## Current correctness boundaries

- Actor state, message receipt, reply, spawned tasks, outgoing messages and timers
  commit atomically. Task result and completion notification also commit atomically.
- Stable logical task IDs survive retry; each attempt receives a new fencing epoch.
- Node registration and individual work claims have separate renewable leases.
  Commit checks both, plus the actor revision, inside the same transaction.
- Duplicate identical commits return the recorded receipt, including after the
  original lease expires. A different completion cannot overwrite the receipt.
- Published blobs are SHA-256 addressed and immutable; they are written before a
  reference commits. All blobs are retained in this prototype: no premature GC.
- Task retries are opt-in (`max_attempts`, default CLI value 1). Success means a
  committed result, not merely that the guest returned. Uncommitted work may repeat.
- The actor restart policy reloads last committed state and retries a failed turn
  with bounded linear backoff. Exhausted messages become failed receipts and the
  next message may proceed. This is not yet a hierarchical OTP supervisor tree.
- Guest modules have no host imports, hence no ambient filesystem, network or DB
  access. The example's work is pure computation. External effect capability APIs
  and their idempotency protocols are not implemented yet.

## Explicit prototype limits

The PostgreSQL adapter uses per-job records and `SKIP LOCKED` claims. Unrelated
jobs can progress concurrently; one actor still processes one turn at a time.
This is **not a throughput benchmark or a claim of linear scaling**. Large objects
and modules remain stored as `bytea`; S3 and retention cleanup are not implemented.
See [coordination migration and range-input limits](docs/scaling-and-inputs.md).
The memory adapter is intentionally not durable across process exits.

Nodes share work through storage polling. `loom node --slots 4` enables four
concurrent task/actor slots; the default remains one. Different actors may run
concurrently, while each actor processes one message turn at a time. See
[multi-actor execution and shutdown](docs/multi-actor.md).
There is no peer RPC, placement scheduler, GPU support, tenant authentication,
membership watch stream, worker autoscaling or distributed channel yet.
The backend interfaces are designed to allow different implementations, but
membership and commit fencing must remain in a shared consistency domain.

Wasm has a 20-million-instruction fuel budget and a 32 MiB per-memory limit (at most
four memories), with 4 MiB input/output and 16 MiB module limits. Compiled modules
are cached without eviction. This is a controlled-module prototype, not a complete
hostile multi-tenant sandbox or an assertion of native-equivalent performance.

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

export LOOM_TEST_DATABASE_URL="$LOOM_DATABASE_URL"
export LOOM_TEST_COMPONENT="$(pwd)/target/research-agent.component.wasm"
cargo test -p loom-conformance --lib -- --ignored --nocapture

cargo build -p loom-cli -p loom-conformance --bin loom --bin hold-claim
python3 scripts/process_recovery.py
```

The process test uses a separate test fixture to claim work, then SIGKILLs it before
execution so takeover is deterministic. Production worker processes then execute
the real Rust/Wasm component. It also verifies that a previously committed task is
not replayed, and that an actor resumes after all its workers were killed while
waiting for approval. The fixture binary is not part of the production CLI.

See [contract details](docs/contracts.md), [current coordination and input verification](docs/scaling-verification.md),
[multi-actor concurrency checks](docs/multi-actor.md), and the
[initial verification record](docs/verification.md).
