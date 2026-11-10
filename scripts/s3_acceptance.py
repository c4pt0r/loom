#!/usr/bin/env python3
"""Verify the local RustFS overlay after building and starting its workers/dashboard.

Run from the repository root: python3 scripts/s3_acceptance.py
Uses the isolated loom-rustfs project; leaves unique test tasks and objects intact.
"""
import json
import os
import subprocess
import time
import uuid
import urllib.parse
import urllib.request

ENV = {**os.environ, "LOOM_DASHBOARD_PORT": "8789"}
COMPOSE = [
    "scripts/local-env.sh", "docker", "compose", "-p", "loom-rustfs",
    "-f", "compose.yaml", "-f", "compose.rustfs.yaml",
]


def run(*args, stdin=None):
    result = subprocess.run(
        COMPOSE + list(args), env=ENV, input=stdin, text=True,
        capture_output=True, timeout=120,
    )
    if result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return result.stdout


def api(path, **query):
    url = "http://127.0.0.1:8789/api/" + path + "?" + urllib.parse.urlencode(query)
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def main():
    sorted_run = json.loads(run(
        "--profile", "tools", "run", "--rm", "-T", "sort",
        "--count", "3000", "--chunk-size", "1000",
    ))
    assert sorted_run["verified"] and sorted_run["task_count"] == 3, sorted_run
    counts = json.loads(run(
        "--profile", "tools", "run", "--rm", "-T", "word-count",
        "--stdin", "--chunk-words", "2", "--reducers", "3", "--print-result",
        stdin="rust wasm rust loom wasm rust\n",
    ))
    assert counts["counts"] == {"rust": 3, "wasm": 2, "loom": 1}, counts
    assert counts["map_task_count"] == 3 and counts["reduce_task_count"] == 3
    actor = "s3-actor-" + uuid.uuid4().hex[:10]
    cli = ("--profile", "tools", "run", "--rm", "-T", "cli", "--namespace", "lab", "repl")
    start_payload = json.dumps({"start": ["RustFS actor task"]})
    approval_payload = json.dumps({"approve": True})
    run(*cli, stdin=(
        "deploy /opt/loom/research-agent.component.wasm\n"
        f"create-actor {actor} $module\n"
        f"send {actor} --id {actor}-start '{start_payload}'\nexit\n"
    ))
    def await_actor(phase):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            record = api("actor", id=actor)
            if json.loads(record["state"]["text"]).get("phase") == phase:
                return record
            time.sleep(0.2)
        raise AssertionError(f"actor did not reach {phase}")
    assert await_actor("awaiting_approval")["revision"] == 2
    run(*cli, stdin=f"send {actor} --id {actor}-approve '{approval_payload}'\nexit\n")
    assert await_actor("done")["revision"] == 3
    # The default Postgres adapter must contain no payloads in this fresh cluster.
    blobs = run("exec", "-T", "postgres", "psql", "-U", "loom", "-d", "loom", "-Atc",
                "SELECT count(*) FROM loom_blobs WHERE namespace='lab'")
    assert blobs.strip() == "0", blobs
    preview = api("object", digest=sorted_run["output"])["preview"]
    assert preview["bytes"] > 0, preview
    # The dashboard uses the selected store for previews, not PostgreSQL blobs.
    assert preview["text"].startswith("["), preview
    assert api("inspect", resource="nodes")["summary"]["nodes"].get("online") == 2
    result = {
        "verified": True, "actor": actor, "sort_run": sorted_run["run"],
        "word_count_run": counts["run"], "word_counts": counts["counts"],
        "postgres_blob_rows": 0, "dashboard_preview_bytes": preview["bytes"],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
