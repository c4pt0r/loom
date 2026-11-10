# Word count verification

Environment: local Colima Linux ARM64 VM, PostgreSQL 17, namespace `lab`, and
workers `node-a` and `node-b`. The updated runtime image contains the native Rust
word-count driver and the Rust-built Wasm Component. Existing workers loaded the
new component without restarting.

## Acceptance results

| Scenario | Result |
| --- | --- |
| Default 16,000 words / 16 tasks | PASS; both workers committed tasks from this run |
| Custom five-word input / three tasks | PASS; hello=2, loom=2, wasm=1 |
| Empty input | PASS; empty map and zero tasks |
| Whitespace-only input | PASS; empty map and zero tasks |
| Case, punctuation, and apostrophes | PASS; exact-token semantics preserved |
| Unicode whitespace and tokens | PASS; complete expected frequency map matched |
| Repeated words across 22 chunks | PASS; every occurrence counted once |
| Byte limit with long words | PASS; twelve 3,000-byte words split into three tasks |
| Oversized word, invalid chunk sizes, invalid UTF-8 | PASS; rejected with nonzero exit status |

For each successful case, the external Python harness checks the full map, total
and distinct word counts, task count, and attempt count. It fetches the final
object through `loom get` and compares that persisted map with the expected result.
The default case matches exact task IDs against both workers' commit logs.

Machine-readable run IDs and result digests:
[word-count-acceptance.json](word-count-acceptance.json).

## Other checks

- `cargo fmt --all --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --locked --workspace`: passed (eight memory contract tests, one
  publication/renewal regression, and one generated binding doctest; two opt-in
  tests ignored).
- Compose configuration validation and runtime image build: passed.
- Existing sorting acceptance harness: all six scenario groups passed with the
  rebuilt image.

The new acceptance scenarios exercise PostgreSQL and Wasm through the application.
No throughput, multi-machine, or word-count crash-recovery claims are made. The
final reduction runs in the driver's memory; worker task execution is distributed.

Reproduce after building the image and starting the workers:

```sh
scripts/local-env.sh python3 scripts/word_count_acceptance.py --report docs/word-count-acceptance.json
scripts/local-env.sh python3 scripts/sort_acceptance.py
```
