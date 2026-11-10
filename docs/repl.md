# Interactive CLI

`loom repl` opens a client session for submitting work and inspecting a namespace.
Workers execute the tasks; the REPL can disconnect without stopping them. It uses
`Backend` and `InspectionStore`, the same interfaces as the CLI and dashboard.

## Start with Docker

From the repository root:

```sh
dc() { scripts/local-env.sh docker compose "$@"; }
dc --profile tools build cli
dc up -d --wait --no-recreate postgres node-a node-b
dc --profile tools run --rm cli --namespace lab repl
```

Keep the TTY enabled for interactive use (do not add `-T`). The image includes the
sorting, word-count, and research-agent Wasm components under `/opt/loom`.

## Submit a sorting task

Enter these commands at the `loom[lab]>` prompt:

```text
nodes
deploy /opt/loom/sort.component.wasm
submit sort-demo $module sort_chunk '[9, 1, 4, 2]' --attempts 3
tasks
status $task
wait $task --timeout 30
result $task
```

The result is `[1,2,4,9]`.

- `deploy` uploads an already compiled Wasm component to the namespace's object
  store and remembers its digest as `$module`. Compilation happens before upload.
- `submit` persists a task specification. `sort-demo` is the task ID, `$module`
  identifies the component, `sort_chunk` selects its exported task, and the JSON
  array is the input. Any eligible worker in the namespace can claim it.
- `tasks` lists execution phases and retry reasons. `status` exposes the task's
  current owner, attempt count, dependencies, output reference, and any failure.
- `wait` polls until completion and remembers the output digest as `$result`.
  `result` loads the task's committed output from the object store.

Use a new task ID for new work. Reusing an ID is an idempotent submission, not a
request to rerun a completed task; a conflicting specification is rejected.
This example submits one task. Submit multiple independent IDs to run work
concurrently across workers.

## Inputs and dependencies

Inline JSON, immutable object input, and task-result dependencies are supported:

```text
put /data/numbers.json
submit from-object $module sort_chunk --object $object
submit after-sort $module sort_chunk --dependency sort-demo
result from-object --output /data/sorted.json
```

`numbers.json` must contain a JSON array for this particular task. File paths are
local to the CLI process. To use host files in Docker, mount their directory:

```sh
dc --profile tools run --rm -v "$PWD/data:/data" cli --namespace lab repl
```

Only one input mode may be supplied. The dependent task stays waiting until its
predecessor succeeds. The worker resolves the referenced input; the REPL does
not download it and resubmit its bytes. `result --output PATH` writes the full
result, including binary output, and overwrites an existing file at that path.

## Inspect tasks, nodes, and actors

```text
tasks --phase failed --limit 10
tasks --search sort --offset 10 --limit 10
nodes
actors
messages --phase waiting
status sort-demo
receipt task:sort-demo
```

Lists are paginated (25 rows by default, at most 100) and include the total match
count. `nodes` includes expired registrations. `task ID` and `members` retain
the original CLI's JSON output. `status` accepts either a task ID or a full job
ID printed by `tasks` / `messages`; `wait` and `result` accept task IDs only.

Actor commands use the same session:

```text
deploy /opt/loom/research-agent.component.wasm
create-actor research-demo $module --handler research --state '{}'
send research-demo --id start '{"start":["Rust Wasm actors","Durable execution"]}'
actor research-demo
messages --search research-demo
```

Once the actor reaches `awaiting_approval`, send its approval message:

```text
send research-demo --id approval '{"approve":true}'
actor research-demo
```

The backend enforces per-actor message ordering and persists actor state. The
REPL does not own actor lifetimes.

## Session behavior

- `help` lists commands; `help submit` or `submit --help` shows command options.
- `$module`, `$object`, `$task`, and `$result` refer to the most recent successful
  corresponding operation. `vars` displays the current bindings. They exist only
  in this session and are replaced only when a command succeeds.
- Quote JSON and paths containing spaces. Bindings replace whole arguments only,
  including quoted whole arguments; there is no expansion inside JSON strings.
  This is a command shell, not a Rust evaluator or an operating-system shell.
- Up/Down recall in-memory history. History is not saved to disk.
- A command error returns to the prompt. Ctrl-C clears the current line; during
  `wait`, it stops waiting without cancelling the task. Submission and other
  mutations finish their acknowledgement before returning to the prompt.
- `exit`, `quit`, or Ctrl-D leaves the REPL. No workers are stopped.
- Namespace and backend are fixed at startup. Restart with different connection
  options to switch them. No database credentials are printed in the prompt.

For a native client, configure a standard PostgreSQL URI DSN and use a namespace
shared with your workers:

```sh
export LOOM_DATABASE_URL='postgresql://loom:password@127.0.0.1:5432/loom'
cargo run -p loom-cli -- --namespace lab repl
```

This DSN is an example for a host-accessible database. The default Compose
PostgreSQL has no host port; use the Docker client above for that cluster.

Commands also work individually, for example `loom --namespace lab tasks`.
For scripted sessions, pipe commands to `repl` (use `-T` with Compose). Prompts
are suppressed, errors are reported while subsequent commands continue, and the
process exits unsuccessfully if any command failed:

```sh
dc --profile tools run --rm -T cli --namespace lab repl <<'COMMANDS'
nodes
tasks --phase failed
exit
COMMANDS
```
