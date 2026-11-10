#!/usr/bin/env python3
"""Verify reference-based input and compatibility against real Compose workers."""
import json
import os
import shlex
from collections import Counter
from word_count_acceptance import dc

namespace = os.getenv("LOOM_NAMESPACE", "lab")
lines = ["你好 loom 你好\n", "wasm loom\n", "café wasm café\n"]
source = "".join(lines)
command = "cat > /tmp/source.txt && loom --namespace " + shlex.quote(namespace) + " put /tmp/source.txt"
digest = dc("run", "--rm", "-T", "--entrypoint", "/bin/sh", "cli", "-c", command, data=source).stdout.strip()
slices = []
offset = 0
for line in lines:
    length = len(line.encode())
    slices.append({"object": {"digest": digest}, "offset": offset, "length": length})
    offset += length


def run_slices(parts, extra=(), check=True):
    command = "cat > /tmp/slices.json && loom-word-count --input-slices /tmp/slices.json " + shlex.join(extra)
    return dc("run", "--rm", "-T", "--entrypoint", "/bin/sh", "word-count", "-c", command,
              data=json.dumps(parts), check=check)


expected = dict(Counter(source.split()))
result = json.loads(run_slices(slices, ["--print-result"]).stdout)
assert result["counts"] == expected and result["map_task_count"] == 3
assert result["total_words"] == sum(expected.values())
print("PASS: UTF-8 source slices produce exact distributed counts", flush=True)

metadata = json.loads(run_slices(slices).stdout)
assert metadata["total_words"] is None and "counts" not in metadata
assert metadata["map_task_count"] == 3
print("PASS: reference-only driver returns metadata without source counting", flush=True)

whole = json.loads(dc("run", "--rm", "-T", "word-count", "--input-object", digest, "--print-result").stdout)
assert whole["counts"] == expected and whole["map_task_count"] == 1
legacy = json.loads(dc("run", "--rm", "-T", "word-count", "--stdin", "--print-result", data=source).stdout)
assert legacy["counts"] == expected
print("PASS: complete object and existing stdin inputs remain compatible", flush=True)

empty = json.loads(run_slices([], ["--print-result"]).stdout)
assert empty["counts"] == {} and empty["map_task_count"] == 0
bad = [{"object": {"digest": digest}, "offset": offset + 1, "length": 0}]
failed = run_slices(bad, ["--timeout-seconds", "20"], check=False)
assert failed.returncode and "range exceeds length" in failed.stderr
print("PASS: empty manifests work and out-of-bounds slices fail clearly", flush=True)
