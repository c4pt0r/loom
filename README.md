# Loom

A Rust runtime for distributed computation and durable agents, inspired by Ray,
Erlang/OTP, and Akka. Rust tasks run as WebAssembly Components on a Rust/Wasmtime
host; durable actors coordinate work and retain state across worker restarts.

## Core primitives

| Primitive | Purpose |
| --- | --- |
| **Task** | Run a computation on a worker, with configurable retries. |
| **Actor** | Maintain durable state and process messages sequentially per actor. |
| **ObjectRef** | Reference immutable inputs, results, and shuffle data. |
| **Supervisor** | Retry failed actor turns with bounded attempts and backoff. |

Different actors and tasks can run concurrently. Actor state, message receipts,
and outgoing work commit atomically. Storage and execution depend on interfaces;
PostgreSQL handles coordination. Objects can use PostgreSQL (default), RustFS,
or AWS S3 through the same interface.

## Quickstart

Requires Docker Engine and Compose. Docker builds the Rust host and Wasm guests.

```sh
git clone https://github.com/c4pt0r/loom.git
cd loom

dc() { scripts/local-env.sh docker compose "$@"; }
dc --profile tools build node-a
dc up -d --wait --no-recreate postgres node-a node-b

dc --profile tools run --rm -T sort
dc --profile tools run --rm -T word-count
```

Sorting verifies its result against a local reference sort. Word count runs
partitioned map/reduce using hash shuffle. Worker concurrency is configurable
with `loom node --slots N` (default: 1).

## Interactive CLI

```sh
dc --profile tools run --rm cli --namespace lab repl
```

Submit tasks, inspect nodes and actors, and read results from an interactive
prompt. Start with `help`; see the [REPL guide](docs/repl.md) for a sorting walkthrough.

## Dashboard

Inspect nodes, task status, failures, actor state, and results at **http://127.0.0.1:8787**:

```sh
dc --profile dashboard build dashboard
dc --profile dashboard up -d --wait --no-recreate postgres dashboard
```

See the [dashboard guide](docs/dashboard.md) for native startup and the local Pi example.

## Examples

Each guide includes setup, Rust code explanations, and recovery behavior.

| Example | What it demonstrates |
| --- | --- |
| [Distributed sorting](docs/quickstart-sort.md) | Parallel Wasm tasks and immutable results. |
| [Word count](docs/quickstart-word-count.md) | Partitioned shuffle and distributed reducers. |
| [Local Pi book summaries](examples/book-summary/README.md) | Parallel chapter summaries followed by a final synthesis. |

The Pi example runs your **locally installed Pi coding agent** through a native
Rust worker, using a separate namespace. Its coordinator and tasks are native
Rust, not Wasm; the model provider is configured through Pi.

## Status

Loom is an experimental prototype. It supports durable actor state, task retries,
lease-based worker recovery, fenced commits, and object-range inputs. Uncommitted
work can repeat; external model calls are not exactly-once.

There is no automatic object cleanup, capability-based scheduling,
or tenant authentication yet. The default PostgreSQL connection has no TLS.
Wasm guests have no filesystem or network access. Native-equivalent performance
and linear scaling have not been established by benchmarks.

## Documentation

[Docker Compose](docs/compose.md) ·
[Contracts](docs/contracts.md) ·
[Object storage](docs/object-store.md) ·
[RustFS and AWS S3](docs/s3.md) ·
[Shuffle](docs/shuffle.md) ·
[Concurrency and recovery](docs/multi-actor.md) ·
[Coordination and input slices](docs/scaling-and-inputs.md)

For local development, use Rust 1.93+ and run `cargo test --workspace`.
See the [Compose guide](docs/compose.md#tests) for PostgreSQL/Wasm integration
and process recovery tests.
