#!/usr/bin/env python3
"""Test actual Compose containers, including SIGKILL and database-volume recovery.

Uses a unique project and namespace. Removes only that project's containers and
volume on exit. Docker/Compose run on the host; no Docker socket is mounted.
Images must already exist: docker compose --profile tests build.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    run_id = uuid.uuid4().hex[:12]
    project = "loom-test-" + run_id
    namespace = "acceptance-" + run_id
    env = dict(os.environ, LOOM_NAMESPACE=namespace)
    compose = ["docker", "compose", "--project-name", project, "--profile", "tests", "--profile", "tools"]
    checks = []
    report = {"project": project, "namespace": namespace, "checks": checks, "status": "FAIL"}

    def dc(*argv, timeout=120, check=True):
        result = subprocess.run(compose + list(argv), cwd=ROOT, env=env,
                                text=True, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"compose {' '.join(argv)} failed:\n{result.stdout}\n{result.stderr}")
        return result.stdout.strip()

    def cli(*argv):
        return dc("exec", "-T", "control", "loom", "--namespace", namespace, *argv, timeout=30)

    def task(name):
        return json.loads(cli("task", name))

    def actor():
        return json.loads(cli("actor", "research"))

    def wait(read, predicate, label, timeout=90):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            last = read()
            if predicate(last):
                return last
            time.sleep(0.25)
        raise AssertionError(f"timed out: {label}: {last}")

    def record(label):
        checks.append(label)
        print("PASS:", label, flush=True)

    def wait_workers_expired():
        wait(lambda: json.loads(cli("members")),
             lambda nodes: not any(n["session"]["node"] in ("node-a", "node-b") for n in nodes),
             "worker leases expire")

    def wait_workers_ready():
        wait(lambda: json.loads(cli("members")),
             lambda nodes: {n["session"]["node"] for n in nodes if not n["draining"]} >= {"node-a", "node-b"},
             "both workers registered")

    try:
        dc("up", "-d", "--wait", "postgres", "control")
        record("PostgreSQL healthy; isolated network and persistent volume created")
        module = cli("deploy", "/opt/loom/research-agent.component.wasm")
        report["module_digest"] = module
        cli("submit", "retained", module, "identity", '"already committed"')
        dc("up", "-d", "node-a")
        baseline = wait(lambda: task("retained"), lambda t: t["outcome"] is not None, "baseline")
        assert "Succeeded" in baseline["outcome"] and baseline["attempts"] == 1
        dc("stop", "node-a")

        cli("submit", "interrupted", module, "identity", '"take over"', "--attempts", "2")
        dc("up", "-d", "claim-fixture")
        wait(lambda: dc("logs", "--no-color", "claim-fixture"),
             lambda log: "task:interrupted" in log, "fixture holds task lease")
        dc("kill", "--signal", "SIGKILL", "claim-fixture")
        dc("up", "-d", "node-b")
        recovered = wait(lambda: task("interrupted"), lambda t: t["outcome"] is not None, "takeover")
        assert "Succeeded" in recovered["outcome"] and recovered["attempts"] == 2
        assert task("retained") == baseline
        record("SIGKILL container takeover: uncommitted task retried; committed task not replayed")

        wait(lambda: json.loads(cli("members")),
             lambda ns: not any(n["session"]["node"] == "node-a" for n in ns), "old node-a expires")
        dc("start", "node-a")
        wait_workers_ready()
        cli("create-actor", "research", module)
        docs = [f"document {i} has five words" for i in range(8)]
        cli("send", "research", "--id", "start", json.dumps({"start": docs}))
        before = wait(actor, lambda a: a["state"].get("phase") == "awaiting_approval", "aggregation")
        assert before["revision"] == 9 and len(before["state"]["reports"]) == 8
        for ref in before["state"]["reports"]:
            assert json.loads(cli("get", ref["digest"]))["words"] == 5
        for service in ("node-a", "node-b"):
            assert "COMMIT task:" in dc("logs", "--no-color", service), f"{service} did not execute tasks"
        record("Two worker containers execute Wasm tasks and aggregate eight immutable results")

        dc("kill", "--signal", "SIGKILL", "node-a", "node-b")
        assert actor() == before
        wait_workers_expired()
        dc("start", "node-a", "node-b")
        wait_workers_ready()
        cli("send", "research", "--id", "approval", '{"approve":true}')
        cli("send", "research", "--id", "approval", '{"approve":true}')
        final = wait(actor, lambda a: a["state"].get("phase") == "done", "approval recovery")
        assert final["revision"] == 10
        assert final["state"]["reports"] == before["state"]["reports"]
        record("Actor survives loss of both workers; duplicate approval commits only once")

        dc("stop", "node-a", "node-b")
        # Recreate the database container, retaining the named volume.
        dc("up", "-d", "--force-recreate", "--wait", "postgres")
        assert actor() == final
        assert task("retained") == baseline
        assert task("interrupted") == recovered
        for ref in final["state"]["reports"]:
            assert json.loads(cli("get", ref["digest"]))["words"] == 5
        wait_workers_expired()
        dc("start", "node-a", "node-b")
        wait_workers_ready()
        assert actor() == final
        record("Database container recreation retains Actor state, receipts, modules and result blobs")

        # A task trap must not bring down the node or corrupt subsequent results.
        cli("submit", "fuel-trap", module, "spin", "null")
        trap = wait(lambda: task("fuel-trap"), lambda t: t["outcome"] is not None, "fuel trap")
        assert "fuel" in trap["outcome"]["Failed"]
        cli("submit", "after-trap", module, "identity", '"healthy"')
        healthy = wait(lambda: task("after-trap"), lambda t: t["outcome"] is not None, "post-trap execution")
        assert "Succeeded" in healthy["outcome"]
        for service in ("node-a", "node-b"):
            dc("exec", "-T", service, "/bin/true")
        record("Wasm fuel exhaustion is contained; workers continue processing tasks")

        report["workers"] = json.loads(cli("members"))
        report["services"] = [
            {key: service.get(key) for key in ("Service", "State", "Health", "Image")}
            for service in (json.loads(line) for line in dc("ps", "--format", "json").splitlines())
        ]
        report["status"] = "PASS"
    except BaseException as exc:
        report["error"] = str(exc)
        report["logs"] = dc("logs", "--no-color", "--tail", "100", check=False)
        print(report["logs"], flush=True)
        raise
    finally:
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        dc("down", "--volumes", "--remove-orphans", check=False)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
