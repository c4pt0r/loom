# Loom

Rust applications compiled to WebAssembly Components, executed by a Rust host.

This is the first executable prototype: Task, durable Actor, ObjectRef, and a
minimal OneForOne restart policy. Flow is deliberately absent. Channel is deferred.

The examples cover distributed sorting and a durable agent state machine that
fans out analysis, aggregates results, waits for approval, and resumes after restart.

## Quickstart：用 Rust 和 Loom 做分布式排序

这篇教程把一组整数切成多个分片，交给两个 Loom 节点并行排序，最后在客户端归并成一个有序数组。你会用到两个原语：**Task** 执行排序，**ObjectRef** 引用输入和结果。

排序逻辑和客户端均用 Rust 编写。排序逻辑编译成 Wasm；节点负责领取任务、执行和保存结果。整个例子通过 Docker Compose 运行，本机只需要 Docker Engine 和 Compose，不需要安装 Rust 或 PostgreSQL。

### 1. 启动两个节点

首次使用先克隆仓库；已有本地源码则直接进入仓库根目录：

```sh
git clone https://github.com/c4pt0r/loom.git
cd loom
```

定义一个便捷命令，兼容这个工作区的 Colima，也兼容已配置的 Docker Desktop：

```sh
dc() { scripts/local-env.sh docker compose "$@"; }

dc --profile tools build node-a
dc up -d --wait --no-recreate postgres node-a node-b
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" members
```

第一次构建需要下载依赖并编译 Rust/Wasmtime，可能需要几分钟。`--no-recreate` 会保留已经运行的节点，它们也可以加载新的排序 Wasm。`members` 应显示 `node-a` 和 `node-b`。如果刚启动时只看到一个节点，稍后再执行一次。

### 2. 运行第一次分布式排序

```sh
dc --profile tools run --rm -T sort
```

默认生成 **16,000 个整数**，拆成 **16 个分片**，每片 1,000 个。客户端会自动部署排序 Wasm，提交所有任务，再等待结果。

终端先显示：

```text
Submitted 16 tasks (run: sort-...)
```

随后输出 JSON 报告，包含：

| 字段 | 含义 |
| --- | --- |
| `count: 16000` | 排序后的元素数量 |
| `task_count: 16` | 提交的排序任务数量 |
| `verified: true` | 结果与独立的本地参考排序完全一致，包括重复值 |
| `output` | 完整有序数组的 ObjectRef digest |
| `tasks` | 每个分片的任务 ID、尝试次数和输出 digest |
| `run` | 本次运行的唯一标识，用于查日志 |

这里的本地参考排序只用于验证；远程任务失败时，客户端会报错退出。

数据经过以下步骤：

```text
Rust 客户端：切片 → 保存输入 ObjectRef → 提交所有 sort_chunk Task
                                     │
                      ┌──────────────┴──────────────┐
                 node-a 执行 Wasm              node-b 执行 Wasm
                      └──────────────┬──────────────┘
                              保存结果 ObjectRef
                                     │
Rust 客户端：等待结果 → 读取有序分片 → 多路归并 → 保存最终 ObjectRef
```

节点从共享任务队列中领取任务，不保证各处理一半，也不保证某个任务落到指定节点。

### 3. 排序自己的数据

输入格式是 JSON 数组，元素为有符号 64 位整数。负数和重复值都支持：

```sh
printf '%s\n' '[9, -2, 7, 7, 0, 3, -8, 1]' |
  dc --profile tools run --rm -T sort --stdin --chunk-size 3 --print-result
```

这会创建 3 个任务。报告中应出现：

```json
"sorted": [-8, -2, 0, 1, 3, 7, 7, 9]
```

从文件读取也一样：

```sh
dc --profile tools run --rm -T sort --stdin --print-result < numbers.json
```

`numbers.json` 需要是同样格式的 JSON 数组。`--print-result` 会打印完整结果；大一点的输入建议省略它，使用报告中的 `output` 获取结果：

```sh
# 将 OUTPUT_DIGEST 替换成报告里的 output 值。
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" get OUTPUT_DIGEST
```

每次运行会生成新的任务 ID，因此可以反复运行，不会与之前的任务冲突。

### 4. 排序任务只有几行 Rust

打开 [`examples/sort-guest/src/lib.rs`](examples/sort-guest/src/lib.rs)。核心代码是：

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

`Task` 定义输入、输出和计算。你不用在任务里选择节点、访问数据库或管理重试。这个任务没有外部副作用，可以安全地重新执行。

同一文件中的 `Guest::task` 将函数名 `sort_chunk` 分派到 `run_task::<SortChunk>`，后者处理 JSON 编解码；`export!` 导出 Wasm Component 接口。这些是当前原型实际需要的入口代码。

### 5. 客户端如何提交和等待

完整代码在 [`examples/sort-client/src/main.rs`](examples/sort-client/src/main.rs)。它先保存分片，再提交任务：

```rust
let object = store.put(serde_json::to_vec(chunk)?).await?;
store.submit(TaskSpec {
    id: id.clone(),
    module: module.clone(),
    function: "sort_chunk".into(),
    input: Input::Object(object),
    max_attempts: 3,
    notify: None,
}).await?;
```

`module` 是部署后得到的 Wasm 模块引用。所有分片都提交之后，客户端轮询 `store.task(&id)`；完成时得到 `TaskOutcome::Succeeded(object)`，再通过 `store.get(&object)` 读取有序分片。

**不要提交一片就立即等待那一片完成**，否则客户端会把可并行的工作串行化。

这里使用当前已实现的 `TaskSpec` / `TaskOutcome` 接口。排序函数只依赖 `&dyn Backend`，其中包括任务与对象存储接口；PostgreSQL 适配器只在客户端 `main` 入口组装，排序算法不依赖 SQL。

归并使用最小堆，从每个分片取当前最小值，复杂度为 `O(N log K)`，其中 `K` 是分片数。普通批处理不需要 Actor 或 Flow。

### 6. 确认任务确实在节点上执行

将报告中的 `run` 值替换进下面命令：

```sh
dc logs --no-color node-a node-b | grep 'sort-你的运行标识'
```

日志中的容器前缀标明执行节点，`COMMIT task:sort-...` 表示任务结果已提交。默认 16 分片示例通常能看到两个节点都执行任务；很小的输入可能全部被一个节点领取。

如果程序一直等待，先检查节点和任务状态：

```sh
dc ps
dc logs --tail 30 node-a node-b
dc --profile tools run --rm -T cli --namespace "${LOOM_NAMESPACE:-lab}" task TASK_ID
```

客户端默认等待最多 120 秒，可用 `--timeout-seconds 300` 调整。超时只结束客户端等待，已提交任务仍可能继续执行。节点中断后，尚未提交结果的任务在租约到期后可以重试，最多 3 次；已提交结果保存在后端。客户端本身没有断点续跑功能。

### 7. 修改代码，再运行

修改 Rust 源码后执行：

```sh
dc --profile tools build node-a
dc --profile tools run --rm -T sort
```

排序客户端和 Wasm 都来自新镜像。现有节点会按新的模块 digest 加载 Wasm，因此仅修改排序示例时不需要重启节点。如果修改的是宿主运行时，停止节点、等待原来的 5 秒注册租约过期，再重建：

```sh
dc stop node-a node-b
sleep 6
dc up -d --wait node-a node-b
```

### 适用范围

这是学习 Loom 提交任务和传递结果的示例：**分片排序分布执行，最终归并在客户端内存中完成**。客户端会保留输入、分片、结果及验证用副本，内存需求为 `O(N)`，不适合直接用于超大文件的外部排序。

示例将每片限制在 1–1,000 个整数，以适应当前 Wasm 的燃料和载荷限制。输入、输出和任务记录会保留在 PostgreSQL 中，原型尚无自动清理。当前后端仍会串行更新同一 namespace 的任务账本，所以这个例子不用于证明相对本地排序的加速比。

用完可以停止环境并保留数据：

```sh
dc down
```

自动验收脚本：`scripts/local-env.sh python3 scripts/sort_acceptance.py`。它会验证默认数据、自定义输入、空数组、重复值和整数边界，并检查默认运行的任务是否由两个节点执行。

这套步骤已在当前双节点 Compose 环境中通过验证，详见[验收记录](docs/sort-verification.md)。

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

Use a development PostgreSQL database. The adapter creates only `loom_ledgers` and
`loom_blobs`; namespaces isolate logical clusters. The prototype connects without
TLS and is intended for a trusted local environment.

```sh
export LOOM_DATABASE_URL='host=127.0.0.1 port=5432 user=YOUR_USER dbname=postgres'

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

- `contracts`: persistence, membership, blob storage and guest-engine interfaces.
- `runtime`: claim → execute → publish object → commit. No DB or Wasmtime dependency.
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

The PostgreSQL reference adapter locks one JSON ledger row per namespace. It is a
correctness baseline, **not a scalable storage design or performance benchmark**.
Large objects and modules are stored separately as `bytea`; S3 is not implemented.
All ledger changes serialize, and jobs/receipts/blobs have no retention cleanup.
The memory adapter is intentionally not durable across process exits.

Nodes share work through storage polling, with one execution slot per process.
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

See [contract details](docs/contracts.md) and [verification record](docs/verification.md).
