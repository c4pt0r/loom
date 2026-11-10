#!/usr/bin/env python3
"""Verify the public sorting Quickstart against the running Compose workers.

Uses an independent Python sort oracle. Requires the rebuilt runtime image and
two workers in the configured namespace. Creates new task IDs, never deletes data.
"""
import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def dc(*args, data=None):
    result = subprocess.run(
        ["docker", "compose", "--profile", "tools", *args],
        cwd=ROOT, input=data, text=True, capture_output=True, timeout=180,
    )
    if result.returncode:
        raise RuntimeError(f"compose failed: {result.stdout}\n{result.stderr}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    checks = []
    default = [(i * 48271) % 100003 - 50001 for i in range(16000)]
    cases = [
        ("default", default, 1000),
        ("custom", [9, -2, 7, 7, 0, 3, -8, 1], 3),
        ("empty", [], 3),
        ("duplicates", [4] * 11, 3),
        ("integer boundaries", [2**63 - 1, -(2**63), 0, -1, 1, -(2**63)], 2),
    ]
    for label, values, chunk in cases:
        command = ["run", "--rm", "-T", "sort", "--print-result"]
        data = None
        if label != "default":
            command += ["--stdin", "--chunk-size", str(chunk)]
            data = json.dumps(values)
        result = json.loads(dc(*command, data=data))
        assert result["sorted"] == sorted(values), label
        assert result["count"] == len(values), label
        assert result["task_count"] == (len(values) + chunk - 1) // chunk, label
        assert result["verified"] is True, label
        assert all(task["attempts"] == 1 for task in result["tasks"]), label
        entry = {"case": label, "count": result["count"],
                 "task_count": result["task_count"], "run": result["run"],
                 "output": result["output"]}
        if label == "default":
            task_ids = {task["id"] for task in result["tasks"]}
            workers = []
            for worker in ("node-a", "node-b"):
                logs = dc("logs", "--no-color", worker)
                committed = {line.split("COMMIT ", 1)[1].strip()
                             for line in logs.splitlines() if "COMMIT " in line}
                if any(f"task:{task_id}" in committed for task_id in task_ids):
                    workers.append(worker)
            assert len(workers) == 2, f"expected both workers to execute default run: {workers}"
            entry["workers"] = workers
        checks.append(entry)
        print(f"PASS: {label} ({len(values)} integers, {result['task_count']} tasks)")

    for bad_input in ('[1.5]', '[9223372036854775808]'):
        result = subprocess.run(
            ["docker", "compose", "--profile", "tools", "run", "--rm", "-T", "sort", "--stdin"],
            cwd=ROOT, input=bad_input, text=True, capture_output=True, timeout=30,
        )
        assert result.returncode != 0 and "stdin must be a JSON array of i64 integers" in result.stderr
    checks.append({"case": "invalid floats and out-of-range integers rejected"})
    print("PASS: invalid inputs rejected")
    if args.report:
        args.report.write_text(json.dumps({"status": "PASS", "checks": checks}, indent=2) + "\n")


if __name__ == "__main__":
    main()
