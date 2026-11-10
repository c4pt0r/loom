#!/usr/bin/env python3
"""Verify distributed word count through Compose using independent expected counts."""
import argparse
from collections import Counter
import json
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
        command = ["run", "--rm", "-T", "word-count"]
        if data is not None:
            command += ["--stdin", "--chunk-words", str(chunk)]
        result = json.loads(dc(*command, data=data).stdout)
        assert result["counts"] == expected, label
        assert result["total_words"] == sum(expected.values()), label
        assert result["unique_words"] == len(expected), label
        assert result["task_count"] == task_count, label
        assert all(task["attempts"] == 1 for task in result["tasks"]), label
        saved = json.loads(dc("run", "--rm", "-T", "cli", "--namespace",
                              os.getenv("LOOM_NAMESPACE", "lab"), "get", result["output"]).stdout)
        assert saved == expected, f"{label}: persisted output differs"
        entry = {"case": label, "total_words": result["total_words"],
                 "task_count": task_count, "run": result["run"], "output": result["output"]}
        if label == "default":
            ids = {"task:" + task["id"] for task in result["tasks"]}
            workers = []
            for worker in ("node-a", "node-b"):
                logs = dc("logs", "--no-color", worker).stdout
                committed = {line.split("COMMIT ", 1)[1].strip()
                             for line in logs.splitlines() if "COMMIT " in line}
                if ids & committed:
                    workers.append(worker)
            assert len(workers) == 2, f"expected both workers in default run: {workers}"
            entry["workers"] = workers
        checks.append(entry)
        print(f"PASS: {label} ({result['total_words']} words, {task_count} tasks)", flush=True)

    oversized = dc("run", "--rm", "-T", "word-count", "--stdin",
                   data="x" * (16 * 1024 + 1), check=False)
    assert oversized.returncode and "a word exceeds the 16 KiB limit" in oversized.stderr
    for chunk in ("0", "1001"):
        invalid = dc("run", "--rm", "-T", "word-count", "--chunk-words", chunk, check=False)
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
