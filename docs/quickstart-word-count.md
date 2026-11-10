# Quickstart: distributed word count

Run Map, shuffle, and Reduce on Loom workers. Each mapper counts its own text
chunk, the host partitions records by key, and reducers independently add the
counts for their partitions. The driver submits the graph and returns references
to the final partitions; it does not merge intermediate results.

## Run the example

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

## Who reads which data?

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

The example driver still reads the complete original text and splits it locally.
Source-level file/object range splitting is separate from shuffle and is not yet
implemented. The original text does not need to be read again for Reduce.

## Map: emit typed values under byte keys

The guest code is in
[`examples/word-count-tasks/src/lib.rs`](../examples/word-count-tasks/src/lib.rs).
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

The driver declares `output: TaskOutput::Shuffle(layout.clone())` on each Map
`TaskSpec`. That tells the host to partition the returned records using
`big_endian_u64(SHA256(key)[0:8]) % partition_count`, publish immutable blocks, and
commit a manifest reference. It replaces the ordinary single-value output mode.

The Rust guest is compiled to Wasm during image build and uploaded at runtime.
Wasm functions are dispatched by `Guest::task`: `count_words` calls
`run_task::<CountWords>`, and `sum_counts` calls `run_reducer::<SumCounts>`.

## Submit reducers before the maps finish

The Rust entry point is
[`examples/word-count/src/main.rs`](../examples/word-count/src/main.rs). After
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

## Reduce: fold blocks on a worker

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

## Read the final dataset

The `output` digest now points to a `PartitionedDataset`, not one global word map:

```text
{ version: 1, layout: { partitions: 4 }, partitions: [ObjectRef, ...] }
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

## Rules, limits, and verification

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
two nodes. See the [shuffle protocol](shuffle.md) and
[verification record](word-count-verification.md) for semantics and coverage.
