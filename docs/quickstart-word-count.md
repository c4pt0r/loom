# Quickstart: distributed word count

Count word frequencies with Rust tasks running as Wasm on Loom workers. This
example follows a simple map/reduce pattern: workers count words in separate text
chunks, and a native Rust driver adds the partial counts together.

## Run the example

From the repository root, build the image and start two workers:

```sh
dc() { scripts/local-env.sh docker compose "$@"; }
dc --profile tools build node-a
dc up -d --wait --no-recreate postgres node-a node-b
dc --profile tools run --rm -T word-count
```

The default input repeats `hello loom hello wasm` 4,000 times. The driver submits
16 tasks and prints a JSON report containing:

```json
{
  "total_words": 16000,
  "unique_words": 3,
  "task_count": 16,
  "counts": {"hello": 8000, "loom": 4000, "wasm": 4000}
}
```

The actual report also includes a unique `run` ID, per-task IDs and attempt counts,
and an `output` digest referencing the saved frequency map.

To supply your own input, use plain UTF-8 text, not JSON:

```sh
printf 'hello loom hello\nwasm loom\n' |
  dc --profile tools run --rm -T word-count --stdin --chunk-words 2
```

This creates three tasks and returns `{"hello": 2, "loom": 2, "wasm": 1}` in
`counts`. Read a text file using the same option:

```sh
dc --profile tools run --rm -T word-count --stdin < book.txt
```

Compose supplies the standard URI DSN
`postgresql://loom:loom-local-only@postgres:5432/loom`. The driver and workers use
the same backend and namespace. Existing workers can load the new component without
restarting; the image build packages the driver and Wasm component together.

## Source layout and execution

| Source | Role |
| --- | --- |
| [`examples/word-count/src/main.rs`](../examples/word-count/src/main.rs) | Native application entry point: read text, submit work, collect and merge counts |
| [`examples/word-count-tasks/src/lib.rs`](../examples/word-count-tasks/src/lib.rs) | Wasm task: count words in one chunk |

```text
word-count driver
  text -> split at word boundaries -> store input objects -> submit tasks
                                                            |
                                              +-------------+-------------+
                                              |                           |
                                       node-a: count_words         node-b: count_words
                                              |                           |
                                              +----- result ObjectRefs ---+
                                                            |
  final frequency map <- add counts by word <- fetch partial maps
```

The driver is an application executable, not a client SDK. It uses the public
`Backend` interfaces. Only `main` constructs the PostgreSQL adapter; the counting
task has no database dependency or ambient filesystem/network access.

## Map: count one text chunk in Rust

The guest defines typed input and output through the `Task` trait:

```rust
struct CountWords;

impl Task for CountWords {
    type Input = String;
    type Output = BTreeMap<String, u64>;

    fn run(text: String) -> Result<Self::Output, String> {
        let mut counts = BTreeMap::new();
        for word in text.split_whitespace() {
            *counts.entry(word.to_owned()).or_insert(0) += 1;
        }
        Ok(counts)
    }
}
```

Each map entry counts one exact token. `BTreeMap` gives deterministic key ordering,
which also makes the serialized result stable. The component's `Guest::task`
implementation dispatches the function name `count_words` to
`run_task::<CountWords>(&input)`. That SDK helper decodes the JSON string, calls the
Rust task, and encodes its map as JSON. `export!` exposes the WIT component ABI.

Rust-to-Wasm compilation happens during `docker compose build`. At runtime the
driver uploads that component through `store.put`; workers fetch it by digest and
Wasmtime compiles it for the worker's CPU.

## Submit chunks without splitting words

`split_chunks` walks `text.split_whitespace()` and builds strings containing at
most `--chunk-words` tokens, with an additional 16 KiB UTF-8 byte limit. It inserts
one space between tokens. Normalizing separators this way preserves token identity
and counts. A single token larger than 16 KiB is rejected before any task is submitted.

For each chunk, the driver saves the input and submits a task:

```rust
let object = store.put(serde_json::to_vec(chunk)?).await?;
store
    .submit(TaskSpec {
        id: id.clone(),
        module: module.clone(),
        function: "count_words".into(),
        input: Input::Object(object),
        max_attempts: 3,
        notify: None,
    })
    .await?;
```

`module` references the uploaded Wasm component. `object` references the chunk's
JSON bytes; the worker resolves it before invoking the guest. Task IDs contain a
unique run ID and chunk index. All chunks are submitted before the driver waits
for results, allowing the workers to process them concurrently.

Submission does not choose a worker. Available nodes claim tasks from the shared
backend, so placement and the number of tasks per node can vary.

## Reduce: add each completed map exactly once in the driver

The driver polls `store.task(&id)`. On `TaskOutcome::Succeeded(object)`, it loads
the partial map and adds its counts:

```rust
let partial: Counts = serde_json::from_slice(&store.get(&object).await?)?;
for (word, count) in partial {
    let total = totals.entry(word).or_default();
    *total = total.checked_add(count).context("word count overflow")?;
}
```

`Counts` is an alias for `BTreeMap<String, u64>`. The checked addition detects
integer overflow. A completed task is removed from the pending set after its map
is merged, so subsequent polling does not add the same task again. Two distinct
chunks may produce the same content digest; each chunk still contributes once.

Addition is independent of task completion order. If a task reaches a terminal
failure, the driver exits with its ID and error. Pure counting tasks allow up to
three attempts. The default 120-second timeout can be changed with
`--timeout-seconds`; a driver timeout does not cancel submitted tasks.

The driver checks that the sum of all returned counts equals the input token count,
then stores the final map through `store.put`. To retrieve it later:

```sh
# Replace OUTPUT_DIGEST with the report's output value.
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" get OUTPUT_DIGEST
```

## Counting rules and limits

- Words are separated by Unicode whitespace, including spaces, tabs, and newlines.
- Matching is case-sensitive: `Loom` and `loom` are different words.
- Punctuation is retained: `loom` and `loom,` are different words.
- Unicode text is preserved without normalization or language-specific segmentation.
  For example, Chinese text without whitespace is one token.
- Empty or whitespace-only input produces an empty map and submits no tasks.

`--chunk-words` accepts 1–1,000. Chunk counting runs on workers, while final reduction
runs in the driver. The driver reads the full input and holds the global frequency
map in memory; this is not an external-memory or distributed-shuffle implementation.
Its reduction state does not survive a driver crash. Task results and final output
objects remain in the backend, which currently has no automatic cleanup.

## Inspect and verify

Use the report's complete `run` value to inspect worker execution:

```sh
# Replace RUN_ID with the full word-count-... value.
dc logs --no-color node-a node-b | grep 'RUN_ID'
```

Run the independent acceptance harness against the running two-worker environment:

```sh
scripts/local-env.sh python3 scripts/word_count_acceptance.py
```

It checks full frequency maps, persisted outputs, default execution on both workers,
empty input, punctuation and case, Unicode whitespace, repeated words across chunks,
byte-limited chunks, and invalid inputs. See the
[verification record](word-count-verification.md) for recorded results.
