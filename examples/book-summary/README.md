# Book summaries with local Pi

This example runs one durable `BookSummaryActor` per book, parallel chapter Tasks,
and one final synthesis Task. Each task invokes **your locally installed Pi coding
agent** as a subprocess. It does not call a model SDK directly or use a hosted
coding agent. Pi inherits your local model/provider authentication; inference may
still run remotely, depending on that configuration.

The example uses a native Rust implementation of Loom's existing `GuestEngine`
interface. Its coordinator and task adapter are compiled into the host binary;
they are **not Wasm components**. The other Loom examples continue to use Wasm.
A versioned object (`loom.example.book-summary.native.v1`) identifies this engine's
application protocol; it is not an uploaded executable. Nodes need this binary
installed. No scheduler, database schema, or existing task wire format changes.

## Quickstart

You need Rust 1.93+, Docker Compose, and a working local Pi installation. Verify
`pi --version` and configure your model/provider with Pi as you normally would.
Run the following commands from the repository root.

### 1. Start storage and build the host

```sh
scripts/local-env.sh docker compose -f compose.book-summary.yaml up -d --wait
cargo build -p loom-book-summary
export LOOM_DATABASE_URL='postgresql://loom:loom-local-only@127.0.0.1:15432/loom'
```

This separate Compose project runs only PostgreSQL and publishes it on loopback
port 15432. It does not alter the existing sorting/word-count stack. Use
`LOOM_BOOK_PG_PORT` to choose another port and update the DSN accordingly.
Coordination and objects use PostgreSQL by default; application code depends
on `Backend` / `ObjectStore` interfaces, not SQL. Submit, worker, and result
commands also honor the shared [RustFS / AWS S3 configuration](../../docs/s3.md).
Use identical storage settings in all processes, including the dashboard.

### 2. Start a local worker

In one terminal, with the same `LOOM_DATABASE_URL`:

```sh
target/debug/loom-book-summary worker \
  --pi "$(command -v pi)" \
  --slots 4 \
  --work-dir work/book-summary
```

To select a particular installation, pass its absolute path, for example
`--pi /Users/dongxu/.local/bin/pi`. Startup logs print the resolved executable.
The Rust worker and Pi processes run on the host, outside Docker.

Use a **dedicated namespace**, default `book-summary`. Stock Wasm workers cannot
execute this example and must not join that namespace: Loom does not yet route
jobs by worker capability. To change it, put `--namespace NAME` before the
subcommand on every invocation. Additional native workers can share that
namespace and storage, each with its own locally installed Pi and credentials.
The loopback Compose setup is for a single host; remote machines need a separately
configured, reachable storage service.

### 3. Submit three chapters

In another terminal, with the same `LOOM_DATABASE_URL`:

```sh
target/debug/loom-book-summary submit \
  --id intro-book \
  --title 'Loom Execution and Recovery' \
  --chapter examples/book-summary/chapters/01-tasks.md \
  --chapter examples/book-summary/chapters/02-actors.md \
  --chapter examples/book-summary/chapters/03-recovery.md \
  --concurrency 3
```

Files are uploaded as immutable objects. Chapter order is the command-line order,
not task completion order. `--concurrency` bounds outstanding chapter tasks per
book; `--slots` bounds simultaneous jobs across the worker. Multiple submitted
books create independent actors and share available slots.

The default is two total attempts per task. Pass `--attempts 1` to disable model
retries. Optionally pin a Pi `--model provider/model`; otherwise each worker uses
its local Pi default, which can change between attempts if you change its config.

### 4. Inspect progress and save the result

```sh
target/debug/loom-book-summary status --id intro-book

target/debug/loom-book-summary status --id intro-book \
  --wait-seconds 600 --output work/book-summary/report.json
```

`status` reads durable state; it does not execute agents. The completed state has
`phase: "completed"`, ordered chapter result references, and a final report
reference. Saving a report before completion exits with an error. A failed book
exits with an error that includes the recorded task failure.

The final JSON contains `title`, `summary`, cited `themes`, ordered `chapters`, and
`open_questions`. Per-attempt local directories contain `source.txt`, `prompt.txt`,
`events.jsonl`, `stderr.log`, and `answer.json`. Durable results live in the object
store, so local directories are not required for recovery. These files contain
source text and model output; manage them as application data.

Ctrl-C drains the worker: it finishes active calls and commits their results,
leaving queued work for another worker. Restart the same command to continue.
Stop the example database with:

```sh
scripts/local-env.sh docker compose -f compose.book-summary.yaml down
```

The volume is retained. No Pi sessions or workers are started by Compose itself.

## How the code works

[`src/lib.rs`](src/lib.rs) defines the coordinator, data structures, and executor
interface. [`src/pi.rs`](src/pi.rs) is the local Pi adapter.
[`src/main.rs`](src/main.rs) only wires storage, CLI commands, and the worker.

```text
submit -> upload chapter sources -> Actor start message
  Actor -> bounded chapter Task intents
    Task -> load one chapter -> local Pi -> validate -> immutable result
      completion message -> Actor remembers result and schedules next chapter
  all chapters complete -> synthesis Task -> local Pi -> validated report
    completion message -> Actor completed
```

`BookEngine::actor` reads a state snapshot and returns a `Decision`. It never calls
Pi. The backend atomically commits the updated actor state, the message receipt,
and the new task intents. Each completed chapter frees one place in the book's
window. The state stores chapter references by index, so out-of-order completion
does not scramble the final book. It creates exactly one synthesis intent when
all chapters have succeeded.

Tasks delegate through an implementation-independent interface:

```rust,ignore
#[async_trait]
pub trait AgentExecutor: Send + Sync {
    async fn run(&self, request: AgentRequest) -> Result<String>;
}
```

`BookEngine::task` resolves source objects through `ObjectStore`, constructs a
prompt, and calls this interface. `LocalPi` implements it by creating a separate
attempt directory and invoking the local executable with `--print --mode json`.
It enables only the read tool and disables extension, MCP, skill, prompt-template,
and project context discovery for this job. Local provider authentication still
comes from Pi. This tool configuration is not an OS filesystem sandbox.

Pi JSON mode is an **event stream**, not the chapter schema. The adapter extracts
the last successfully completed assistant answer. The application then parses
that answer as a chapter summary or book report. Chapter results must identify
the correct chapter and cite valid numbered paragraphs. The final report must
cover every chapter exactly once in input order and use valid chapter IDs for
its themes. Structural checks do not prove factual accuracy.

The runtime uploads validated task results before its fenced commit publishes the
reference. Actor state contains source/result references and progress, not a Pi
process, model session, or database connection. Pi calls do not hold a database
transaction open. The example starts a fresh Pi session for each attempt.

## Whole objects, slices, and direct text

Use `--manifest chapters.json` instead of `--chapter` to submit a JSON array:

```json
[
  {
    "id": "one",
    "title": "Chapter One",
    "source": {"Inline": "Text passed directly by the caller."}
  },
  {
    "id": "two",
    "title": "Chapter Two",
    "source": {"Object": {"digest": "REPLACE_WITH_OBJECT_DIGEST"}}
  },
  {
    "id": "three",
    "title": "Chapter Three",
    "source": {
      "ObjectSlice": {
        "object": {"digest": "REPLACE_WITH_OBJECT_DIGEST"},
        "offset": 100,
        "length": 800
      }
    }
  }
]
```

References must exist in the same logical object store/namespace. Offsets and
lengths are bytes, not characters; the caller must align chapter and UTF-8
boundaries. Direct text is uploaded by `submit` so the persisted actor state stays
compact. Existing whole-object inputs remain supported. A chapter task reads only
its assigned source, not all book data.

## Failure and limits

- Repeating the same submission with the same ID and content is idempotent.
  Reusing the ID with changed content is rejected. Use a new ID for a new book.
- Successful committed chapter results survive worker restarts and are reused.
  An uncommitted external call may be repeated and billed again after takeover.
  Fencing protects commits; it does not make model requests exactly-once.
- Pi process errors, timeout, malformed JSON, and invalid citations consume a
  task attempt. On exhaustion the book enters `failed` and no synthesis starts.
  Already submitted work may still finish; failed books are not automatically
  reopened. Inspect the failure and submit a new ID after correcting the input.
- Default Pi timeout is 300 seconds per attempt. Timeout and in-process
  cancellation kill the direct Pi child. Killing the Rust process with SIGKILL
  bypasses destructors and can leave its Pi child running; the lease still expires
  and fences stale commits. An already sent remote model request cannot be undone.
- The first version accepts 1..64 chapters, each at most 256 KiB, and chapter
  answers up to 64 KiB. All chapter summaries must fit the synthesis model's
  context. There is no automatic hierarchical summarization, original-source
  lookup during synthesis, session migration, or model-level rate limiter yet.
- Sources, results, and local attempt directories have no automatic garbage
  collection. Credentials stay in the local Pi environment/configuration and are
  not persisted in task input.

## Verification

```sh
cargo test -p loom-book-summary
cargo clippy -p loom-book-summary --all-targets -- -D warnings

# Optional PostgreSQL-backed restart/idempotency test (no model calls):
LOOM_TEST_DATABASE_URL="$LOOM_DATABASE_URL" cargo test -p loom-book-summary \
  --test workflow -- --include-ignored
```

Deterministic tests use an injected executor and local process fixtures; they make
no model calls. They cover two books running concurrently, window bounds, ordered
results, direct/object/slice inputs, retry exhaustion, invalid citations, duplicate
start, lease takeover, committed-result reuse, stale commit rejection, local
executable argument handling, timeout, and child cancellation.

A local end-to-end run also completed with Pi 1.3.0: three concurrent chapter
calls followed by one synthesis call, PostgreSQL-backed state, and a completed
actor at revision 5. A separate PostgreSQL-backed test verified that reopening
the backend, starting another worker, and repeating the submission leaves the
completed state unchanged and invokes no additional agent calls. The checked-in chapters are the inputs for that run.
