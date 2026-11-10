# Row-oriented coordination and object slices

Loom keeps its implementation-independent interfaces while separating scheduling
records and allowing workers to fetch bounded input ranges. Existing complete
object inputs continue to work without changes.

## PostgreSQL coordination

The adapter now stores nodes, actors, jobs, and dependency edges in separate
tables. A task operation reads its own record and the records needed to validate
its dependencies, ownership, or notification. It no longer reads and rewrites a
namespace-wide JSON ledger.

Eligible work is selected with `FOR UPDATE SKIP LOCKED`. A worker can claim another
job while a different transaction holds one job's row lock. Pending jobs have
indexes for scheduling order and actor queues; dependency edges allow PostgreSQL
to skip tasks whose inputs are unfinished. Failed dependencies remain eligible
for failure propagation even if another input is still pending.

Per-job and per-actor transaction locks protect idempotency keys, including keys
that have not been inserted yet. Node rows are shared-locked during ownership
checks, preventing an incarnation change from racing with a commit. The database
clock is read after acquiring the relevant locks. Actor commits still atomically
write state, receipts, child tasks, and outgoing messages. Only serialization
failures and deadlocks known to have aborted are retried automatically; ambiguous
connection failures retain the existing receipt/retry contract.

Each adapter uses four database connections with bounded per-connection access.
Independent operations can overlap through the same adapter. Bootstrap locks are
used only for schema initialization or migration; established namespaces bypass
bootstrap DDL. Actor queues still intentionally serialize one turn per actor.

The pure reference state machine remains shared with the memory adapter. The
PostgreSQL adapter loads a scoped state snapshot and applies a transition to the
selected job, then persists only changed rows in one transaction. Dependency
records are context, not additional jobs that may accidentally be claimed.

## Upgrade from the original ledger

Stop every old worker and client writing to a namespace before upgrading. The new
adapter migrates a version 1 ledger transactionally on first connection:

1. Copy nodes, actors, jobs, receipts, leases, and dependencies into their tables.
2. Preserve relative job order using the database sequence.
3. Mark the namespace as schema version 2 in the same transaction.

The old JSON body remains as a migration snapshot. It is no longer updated and
must not be used as current state or as an automatic rollback source after new
writes. Old workers reject schema version 2; mixed-version operation is unsupported.
The object table and object references do not change. Existing Hash shuffle
layouts and manifests retain their compatibility behavior.

## Complete objects and slices

```rust
// Existing code: the worker loads the complete object as guest input.
let input = Input::Object(object.clone());

// New code: the worker loads exactly the selected byte range.
let input = Input::ObjectSlice(ObjectSlice {
    object,
    offset: 4096,
    length: 2048,
});
```

Offsets and lengths are byte counts. Ranges are half-open: `[offset, offset + length)`.
Overflow, missing objects, short reads, and ranges beyond the object's end fail
explicitly. An empty range at EOF is valid. The runtime rejects slices larger than
its 4 MiB input limit before fetching data. It passes the selected bytes directly
to the guest, without JSON encoding or implicit text decoding.

`ObjectStore::get_range` has a default implementation using `get` and slicing, so
existing third-party implementations still compile. `Stores` forwards range calls
to the selected object provider. PostgreSQL overrides range reads with `substring`
and returns only the requested bytes over the connection; the memory object-only
adapter copies only the slice. The fallback may still load the whole object.
PostgreSQL may access/decompress additional stored bytes internally; this is not a
claim of bounded disk IO. Range reads rely on the provider's immutable-object
integrity; they do not recompute the full object's SHA-256 on each partial read.

A slice is a generic byte range, not a record parser. The producer must choose
boundaries appropriate for the guest's input format: for example, complete lines
of UTF-8 text or individually encoded records. Arbitrarily slicing a JSON document
or the middle of a word does not create independently valid inputs. Intentional
repeated/overlapping ranges are separate inputs; Loom does not deduplicate them.

## Word Count without driver-side source reads

The existing sample and `--stdin` modes remain unchanged. They split text locally
and submit complete JSON-encoded chunk objects to the original `count_words`
function.

The new modes consume existing raw UTF-8 source objects:

```sh
loom --namespace lab put source.txt
# Save the printed digest in a slices.json manifest, or pass the complete object:
loom-word-count --input-object OBJECT_DIGEST --print-result
loom-word-count --input-slices slices.json --print-result
```

Both commands use the existing database and namespace configuration. For example,
set `LOOM_DATABASE_URL` and `LOOM_NAMESPACE=lab` before running them locally.
`loom-word-count` also accepts `--component` for a locally built component.
The bundled component in the Compose image includes the new task entry point.

For the exact source `hello loom\nhello wasm\n`, the reference manifest is:

```json
[
  {"object":{"digest":"OBJECT_DIGEST"},"offset":0,"length":11},
  {"object":{"digest":"OBJECT_DIGEST"},"offset":11,"length":11}
]
```

Each line ends between words. The input producer uploads the source once and
supplies the manifest; the driver reads only this small manifest. It creates one
Map per slice, passing `Input::ObjectSlice`. Workers fetch their ranges and call
`count_words_text`, which decodes UTF-8 and uses the existing word-count logic.
Complete raw objects use `Input::Object` with the same new function. Hash shuffle
and partition reducers are unchanged.

The example limits each slice to 16 KiB and the manifest to 1,024 entries. It does
not automatically discover text boundaries in a remote object. Without
`--print-result`, source-reference modes report `total_words: null` because the
driver has not read or counted the source. With that option, totals are calculated
from final result partitions. No Map output is downloaded by the driver.

## Remaining limits

PostgreSQL remains one shared service for coordination and, by default, payload
storage. These changes remove a namespace-wide lock and avoid source download by
the driver in reference modes; they do not establish linear scaling or remove
all storage bottlenecks. Claim polls examine at most 64 locked/busy candidates;
very large actor queues and dependency sets still require work. There is no
automatic source splitting, range-aware encoding recovery, object garbage
collection, or S3 adapter yet. Shuffle block prefetch and large-accumulator spill
are separate follow-up work.

See [verification results](scaling-verification.md) for tested behavior and reproduction commands.
