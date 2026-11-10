# Distributed shuffle Word Count verification

Environment: local Colima Linux ARM64 VM, PostgreSQL 17, namespace `lab`, and
upgraded workers `node-a` and `node-b`. The driver, map task, and reducer task are
Rust; both guest functions execute through Wasmtime on the workers.

## Application acceptance

All eleven scenario groups in `scripts/word_count_acceptance.py` passed:

| Scenario | Result |
| --- | --- |
| Default 16,000 words | PASS; 16 Maps + 4 Reducers |
| 50,000 words, 500 distinct keys | PASS; 100 Maps + 4 Reducers; both workers executed both stages |
| Custom five-word input | PASS; hello=2, loom=2, wasm=1 |
| Empty input | PASS; zero Maps, four empty Reducers |
| Whitespace-only input | PASS; empty final partitions |
| Case, punctuation, apostrophes | PASS; exact-token semantics |
| Unicode whitespace and tokens | PASS; expected frequency map |
| Words repeated across 22 chunks | PASS; no missing or double-counted occurrences |
| Byte-limited chunks | PASS; twelve 3,000-byte words split across three Maps |
| Metadata-only driver, one reducer | PASS; no counts returned by default; stored partition is correct |
| Invalid input | PASS; oversized words, chunk/partition bounds, and invalid UTF-8 rejected |

For every successful case, the external Python harness verifies the complete
frequency map, input total, distinct keys, map/reduce task counts, and one attempt
per task. It fetches the saved `PartitionedDataset` and each final partition,
checks key ownership using Python's SHA-256, and compares the union with independent
expected counts.

For the default run it additionally reads the actual mapper manifests and blocks,
checks byte/record counts, and independently verifies every intermediate record's
partition. Worker commit logs are matched to exact task IDs. The larger workload
checks that both stages actually ran on both workers; small jobs may legally be
claimed entirely by one node and have no fairness guarantee.

Machine-readable results: [word-count-acceptance.json](word-count-acceptance.json).
Generic protocol/failure checks: [shuffle-verification.md](shuffle-verification.md).

The driver reads original text to make input chunks. Normal execution does not
read intermediate or final data; `--print-result` fetches only final partitions for
presentation. No throughput or multi-machine scaling claim is made.
