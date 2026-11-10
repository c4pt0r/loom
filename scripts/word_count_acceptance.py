#!/usr/bin/env python3
"""Verify distributed word count through Compose using independent expected counts."""
import argparse
from collections import Counter
import json
import hashlib
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
BASE = ["docker", "compose", "--profile", "tools"]


def dc(*args, data=None, check=True):
    result = subprocess.run(BASE + list(args), cwd=ROOT, input=data, text=True,
                            capture_output=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(f"compose failed:\n{result.stdout}\n{result.stderr}")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    checks = []
    cases = [
        ("default", None, 1000, {"hello": 8000, "loom": 4000, "wasm": 4000}, 16),
        ("parallel reducers", " ".join(f"token{i:03d}" for i in range(500)) + " ", 500,
         {f"token{i:03d}": 100 for i in range(500)}, 100),
        ("custom", "hello loom hello\nwasm loom\n", 2,
         {"hello": 2, "loom": 2, "wasm": 1}, 3),
        ("empty", "", 2, {}, 0),
        ("whitespace", " \t\n\r\u2003", 2, {}, 0),
        ("case and punctuation", "Loom loom loom, LOOM! can't can't", 2,
         {"Loom": 1, "loom": 1, "loom,": 1, "LOOM!": 1, "can't": 2}, 3),
        ("Unicode whitespace and tokens", "你好\u2003世界\u00a0你好\nCAFÉ café café 😀", 2,
         {"你好": 2, "世界": 1, "CAFÉ": 1, "café": 2, "😀": 1}, 4),
        ("cross-chunk counts", "alpha beta alpha gamma " * 37, 7,
         dict(Counter(["alpha", "beta", "alpha", "gamma"] * 37)), 22),
        ("byte-limited chunks", ("x" * 3000 + " ") * 12, 1000, {"x" * 3000: 12}, 3),
    ]
    for label, data, chunk, expected, task_count in cases:
        if label == "parallel reducers":
            data *= 100
        command = ["run", "--rm", "-T", "word-count", "--print-result"]
        if data is not None:
            command += ["--stdin", "--chunk-words", str(chunk)]
        result = json.loads(dc(*command, data=data).stdout)
        assert result["counts"] == expected, label
        assert result["total_words"] == sum(expected.values()), label
        assert result["unique_words"] == len(expected), label
        assert result["map_task_count"] == task_count, label
        assert result["reduce_task_count"] == 4, label
        assert result["task_count"] == task_count + 4, label
        assert all(task["attempts"] == 1 for task in (result["map_tasks"] + result["reduce_tasks"])), label
        saved = json.loads(dc("run", "--rm", "-T", "cli", "--namespace",
                              os.getenv("LOOM_NAMESPACE", "lab"), "get", result["output"]).stdout)
        assert saved["version"] == 1 and saved["layout"] == {"partitions": 4, "strategy": "Hash"}
        assert saved["partitions"] == result["partitions"]
        combined = {}
        for partition, reference in enumerate(saved["partitions"]):
            part = json.loads(dc("run", "--rm", "-T", "cli", "--namespace",
                                os.getenv("LOOM_NAMESPACE", "lab"), "get", reference["digest"]).stdout)
            for key, count in part.items():
                route = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big") % 4
                assert route == partition, f"{label}: wrong final partition"
                assert key not in combined, f"{label}: key in multiple partitions"
                combined[key] = count
        assert combined == expected, f"{label}: persisted partitions differ"
        entry = {"case": label, "total_words": result["total_words"],
                 "map_task_count": task_count, "reduce_task_count": 4, "run": result["run"], "output": result["output"]}
        if label == "default":
            # Check physical routing using Python SHA-256, independent of the Rust implementation.
            for task in result["map_tasks"]:
                manifest = json.loads(dc("run", "--rm", "-T", "cli", "--namespace",
                                         os.getenv("LOOM_NAMESPACE", "lab"), "get", task["output"]).stdout)
                assert manifest["version"] == 2 and manifest["layout"] == {"partitions": 4, "strategy": "Hash"}
                for partition, blocks in enumerate(manifest["partitions"]):
                    for block in blocks:
                        raw = dc("run", "--rm", "-T", "cli", "--namespace",
                                 os.getenv("LOOM_NAMESPACE", "lab"), "get", block["object"]["digest"]).stdout.rstrip("\n")
                        assert len(raw.encode()) == block["bytes"] <= 65536
                        records = json.loads(raw)
                        assert len(records) == block["records"]
                        for record in records:
                            route = int.from_bytes(hashlib.sha256(bytes(record["key"])).digest()[:8], "big") % 4
                            assert route == partition
        if label in ("default", "parallel reducers"):
            for stage in ("map", "reduce"):
                ids = {"task:" + task["id"] for task in result[stage + "_tasks"]}
                workers = []
                for worker in ("node-a", "node-b"):
                    logs = dc("logs", "--no-color", worker).stdout
                    committed = {line.split("COMMIT ", 1)[1].strip()
                                 for line in logs.splitlines() if "COMMIT " in line}
                    if ids & committed:
                        workers.append(worker)
                if label == "parallel reducers":
                    assert len(workers) == 2, f"expected both workers under load for {stage}: {workers}"
                entry[stage + "_workers"] = workers
        checks.append(entry)
        print(f"PASS: {label} ({result['total_words']} words, {task_count} maps + 4 reducers)", flush=True)

    metadata = json.loads(dc("run", "--rm", "-T", "word-count", "--stdin", "--reducers", "1",
                             data="a a b").stdout)
    assert "counts" not in metadata and "unique_words" not in metadata
    assert len(metadata["partitions"]) == 1
    final = json.loads(dc("run", "--rm", "-T", "cli", "--namespace", os.getenv("LOOM_NAMESPACE", "lab"),
                         "get", metadata["partitions"][0]["digest"]).stdout)
    assert final == {"a": 2, "b": 1}
    checks.append({"case": "metadata-only driver and single reducer"})
    print("PASS: metadata-only driver and single reducer", flush=True)

    oversized = dc("run", "--rm", "-T", "word-count", "--stdin",
                   data="x" * (16 * 1024 + 1), check=False)
    assert oversized.returncode and "a word exceeds the 16 KiB limit" in oversized.stderr
    for chunk in ("0", "1001"):
        invalid = dc("run", "--rm", "-T", "word-count", "--chunk-words", chunk, check=False)
        assert invalid.returncode and "invalid value" in invalid.stderr
    for partitions in ("0", "257"):
        invalid = dc("run", "--rm", "-T", "word-count", "--reducers", partitions, check=False)
        assert invalid.returncode and "invalid value" in invalid.stderr
    invalid_utf8 = subprocess.run(BASE + ["run", "--rm", "-T", "word-count", "--stdin"],
                                  cwd=ROOT, input=b"\xff", capture_output=True, timeout=30)
    assert invalid_utf8.returncode and b"stdin must be valid UTF-8" in invalid_utf8.stderr
    checks.append({"case": "oversized words, invalid chunk sizes, and invalid UTF-8 rejected"})
    print("PASS: invalid inputs rejected", flush=True)
    if args.report:
        args.report.write_text(json.dumps({"status": "PASS", "checks": checks}, indent=2) + "\n")


if __name__ == "__main__":
    main()
