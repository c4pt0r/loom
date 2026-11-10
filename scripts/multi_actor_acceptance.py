#!/usr/bin/env python3
"""Real Wasm actors on two four-slot workers in an isolated Compose project."""
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
run = uuid.uuid4().hex[:12]
namespace = "actors-" + run
project = "loom-actors-" + run
env = dict(os.environ, LOOM_NAMESPACE=namespace, LOOM_SLOTS="4")
base = ["docker", "compose", "--project-name", project, "--profile", "tests", "--profile", "tools"]


def dc(*args):
    result = subprocess.run(base + list(args), cwd=ROOT, env=env, text=True,
                            capture_output=True, timeout=180)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout.strip()


def cli(*args):
    return dc("exec", "-T", "control", "loom", "--namespace", namespace, *args)


def wait(predicate):
    end = time.monotonic() + 90
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.1)
    raise AssertionError("multi-actor scenario timed out")


try:
    dc("up", "-d", "--wait", "postgres", "control")
    module = cli("deploy", "/opt/loom/research-agent.component.wasm")
    documents = {}
    for n in range(16):
        actor = f"research-{n}"
        docs = [f"{actor} document {i} has unique content" for i in range(8)]
        documents[actor] = docs
        cli("create-actor", actor, module)
        cli("send", actor, "--id", actor + "-start", json.dumps({"start": docs}))
    dc("up", "-d", "--wait", "node-a", "node-b")
    for worker in ("node-a", "node-b"):
        assert "SLOTS 4" in dc("logs", "--no-color", worker)
    states = {}
    def collected():
        for actor in documents:
            if actor not in states:
                state = json.loads(cli("actor", actor))
                if state["state"].get("phase") == "awaiting_approval":
                    states[actor] = state
                assert state["state"].get("phase") != "failed"
        return len(states) == len(documents)
    wait(collected)
    for actor, docs in documents.items():
        snapshot = states[actor]
        assert snapshot["revision"] == 9
        assert snapshot["state"]["failures"] == []
        reports = [json.loads(cli("get", r["digest"])) for r in snapshot["state"]["reports"]]
        assert sorted(r["text"] for r in reports) == sorted(docs)
        assert all(r["words"] == len(r["text"].split()) for r in reports)
    print("PASS: 16 isolated Wasm actors aggregate 128 tasks on two four-slot workers", flush=True)
    for actor in documents:
        for _ in range(2):
            cli("send", actor, "--id", actor + "-approval", '{"approve":true}')
    def approved():
        return all(json.loads(cli("actor", actor))["state"].get("phase") == "done" for actor in documents)
    wait(approved)
    for actor in documents:
        assert json.loads(cli("actor", actor))["revision"] == 10
    print("PASS: duplicate approval messages advance each actor exactly once", flush=True)
    dc("stop", "node-a", "node-b")
    for actor in documents:
        snapshot = json.loads(cli("actor", actor))
        assert snapshot["revision"] == 10 and snapshot["state"]["phase"] == "done"
    print("PASS: all actor states persist after both multi-slot workers stop", flush=True)
finally:
    dc("down", "--volumes", "--remove-orphans")
