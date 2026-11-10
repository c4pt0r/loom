# Dashboard verification

Verified against the local PostgreSQL-backed runtime and the existing local-Pi
book-summary results. No additional model calls were made for dashboard testing.

- `cargo test --workspace`: 39 tests passed; 9 external-service tests ignored in
  the default run.
- Full `loom-conformance` run with PostgreSQL and the compiled Wasm component:
  38 tests passed, including the ignored integration tests.
- Dashboard HTTP tests passed, including read-only routing, host validation,
  missing resources, invalid queries, bounded previews, and a UTF-8 character
  split at the preview boundary.
- `cargo clippy --workspace --all-targets -- -D warnings`, Rust formatting,
  JavaScript syntax, and documentation-link checks passed.
- Docker image builds with the repository's Rust 1.93 toolchain.

Browser checks used actual data:

- Main `lab` namespace: 2 online nodes, 1,016 stored tasks, pagination from records
  1–25 to 26–50, and filtering the 5 historical failed tasks.
- A failed reducer exposed its upstream task ID and the actual
  `object range exceeds length` failure, plus its input and receipt.
- The `book-summary` namespace showed the completed actor at revision 5; following
  its report ObjectRef opened the real Pi-generated summary.
- A controlled dashboard-server shutdown retained the last snapshot with an
  explicit stale-data warning. Restart and refresh cleared the warning.
- Searching for a nonexistent node showed an empty-result state. Clearing the
  search restored the node list.

Counts above are observations from the verification run, not fixed UI content.
The dashboard does not collect CPU, memory, utilization, or historical latency.
