#!/usr/bin/env python3
"""Public-CLI acceptance test against a disposable PostgreSQL namespace.

Requires LOOM_DATABASE_URL. Does not create/drop databases or touch other namespaces.
Build: cargo build -p loom-cli -p loom-conformance --bin loom --bin hold-claim
"""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "target/debug/loom"
COMPONENT = ROOT / "target/research-agent.component.wasm"
NAMESPACE = "process-" + uuid.uuid4().hex
PROCESSES = []
LOGS = []


def command(*args):
    p = subprocess.run([str(BIN), "--namespace", NAMESPACE, *args],
                       capture_output=True, text=True, check=True, timeout=15)
    return p.stdout.strip()


def wait_for(read, predicate, label, timeout=30):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = read()
        if predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"timed out: {label}: {last}")


def node(name):
    log = tempfile.TemporaryFile(mode="w+")
    LOGS.append(log)
    p = subprocess.Popen([str(BIN), "--namespace", NAMESPACE, "node", "--id", name,
                          "--lease-ms", "3000"], stdout=log, stderr=log)
    PROCESSES.append(p)
    return p


def stop(p, force=False):
    if p.poll() is None:
        p.send_signal(signal.SIGKILL if force else signal.SIGINT)
        p.wait(timeout=10)


def task_status(name):
    return json.loads(command("task", name))


def main():
    assert os.environ.get("LOOM_DATABASE_URL"), "LOOM_DATABASE_URL required"
    module = command("deploy", str(COMPONENT))
    command("submit", "retained", module, "identity", '"committed once"')
    first = node("first")
    retained = wait_for(lambda: task_status("retained"), lambda x: x["outcome"] is not None, "baseline task")
    assert "Succeeded" in retained["outcome"] and retained["attempts"] == 1
    stop(first)

    command("submit", "interrupted", module, "identity", '"recover me"', "--attempts", "2")
    fixture = subprocess.Popen([str(ROOT / "target/debug/hold-claim"), NAMESPACE],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    PROCESSES.append(fixture)
    selector = selectors.DefaultSelector()
    selector.register(fixture.stdout, selectors.EVENT_READ)
    assert selector.select(timeout=10), "claim fixture did not become ready"
    assert fixture.stdout.readline().strip() == "task:interrupted"
    selector.close()
    stop(fixture, force=True)

    second, third = node("second"), node("third")
    recovered = wait_for(lambda: task_status("interrupted"), lambda x: x["outcome"] is not None, "takeover")
    assert "Succeeded" in recovered["outcome"] and recovered["attempts"] == 2
    assert task_status("retained") == retained, "committed task was replayed"

    command("create-actor", "research", module)
    command("send", "research", "--id", "start", '{"start":["one two","three four five"]}')
    before = wait_for(lambda: json.loads(command("actor", "research")),
                      lambda x: x["state"].get("phase") == "awaiting_approval", "fanout and aggregation")
    assert before["revision"] == 3 and len(before["state"]["reports"]) == 2
    for ref in before["state"]["reports"]:
        result = json.loads(command("get", ref["digest"]))
        assert result["words"] in (2, 3)
    stop(second, force=True)
    stop(third, force=True)
    fourth = node("fourth")
    # New worker process, same logical actor; repeat external approval with same ID.
    command("send", "research", "--id", "approval", '{"approve":true}')
    command("send", "research", "--id", "approval", '{"approve":true}')
    final = wait_for(lambda: json.loads(command("actor", "research")),
                     lambda x: x["state"]["phase"] == "done", "approval after restart")
    assert final["revision"] == 4, "duplicate approval modified state twice"
    assert final["state"]["reports"] == before["state"]["reports"]
    stop(fourth)
    print(json.dumps({"status": "PASS", "namespace": NAMESPACE,
                      "checks": ["SIGKILL uncommitted claim then takeover", "committed task not replayed",
                                 "two nodes sharing durable state", "Rust Wasm fanout and ObjectRefs",
                                 "actor recovered across process restart", "duplicate approval deduplicated"]}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        for log in LOGS:
            log.seek(0)
            print(log.read())
        raise
    finally:
        for process in PROCESSES:
            stop(process, force=True)
        for log in LOGS:
            log.close()
