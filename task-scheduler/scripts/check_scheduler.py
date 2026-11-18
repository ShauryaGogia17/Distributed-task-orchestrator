#!/usr/bin/env python3
"""Verify two Docker schedulers, delayed/cron execution, and optional killed-leader takeover."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx

from shared.config import PROJECT_ROOT, get_settings
from shared.kafka import consumer_context
from shared.redis import create_redis_client


async def docker(*arguments: str) -> str:
    """Read only needed Docker fields; never print container environment credentials."""
    process = await asyncio.create_subprocess_exec(
        "docker",
        *arguments,
        cwd=PROJECT_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    if process.returncode:
        raise RuntimeError(f"Docker {arguments[0]} failed; inspect the service logs")
    return stdout.decode().strip()


async def nodes(http: httpx.AsyncClient) -> list[dict]:
    """Discover random loopback ports assigned to the two scheduler replicas."""
    containers = (await docker("compose", "ps", "-q", "scheduler")).splitlines()
    if len(containers) != 2:
        raise RuntimeError(f"Expected 2 scheduler containers, found {len(containers)}")
    snapshots = []
    for container in containers:
        mapping = json.loads(
            await docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", container)
        )
        port = mapping["8001/tcp"][0]["HostPort"]
        base = f"http://127.0.0.1:{port}"
        response = await http.get(f"{base}/health")
        response.raise_for_status()
        status = response.json()
        text = (await http.get(f"{base}/metrics")).text
        value = next(line for line in text.splitlines() if line.startswith("scheduler_is_leader{"))
        if float(value.rsplit(" ", 1)[1]) != int(status["leader"]):
            raise RuntimeError("Leader health and metric disagree")
        snapshots.append({"container": container, "base": base, **status})
    if sum(node["leader"] for node in snapshots) != 1:
        raise RuntimeError("Expected exactly one active leader")
    return snapshots


async def check(kill_leader: bool) -> None:
    """Leave sample tasks for inspection and always pause the sample cron definition."""
    configured = get_settings()
    redis = create_redis_client(configured.redis)
    prefix = f"scheduler-check-{uuid4().hex}"
    stopped = None
    restart_policy = None
    schedule_id = None
    async with httpx.AsyncClient(timeout=12) as http:
        api = f"http://127.0.0.1:{configured.api.port}"
        try:
            replicas = await nodes(http)
            print("[OK] Two healthy schedulers; leader metrics are 1 and 0")
            async with consumer_context("tasks.pending", group_id=prefix) as consumer:
                due_at = datetime.now(UTC) + timedelta(seconds=3)
                response = await http.post(
                    f"{api}/tasks",
                    json={
                        "type": "generate_report",
                        "payload": {"data": [1, 2, 3], "seconds": 0},
                        "scheduled_at": due_at.isoformat(),
                        "idempotency_key": prefix,
                    },
                )
                response.raise_for_status()
                task = response.json()
                if task["status"] != "SCHEDULED" or task["attempts"] != 0:
                    raise RuntimeError("Future task was not held in SCHEDULED")
                identifier = task["id"]
                if await redis.zscore(configured.scheduler.queue_key, identifier) is None:
                    raise RuntimeError("Future task is missing from the Redis sorted set")
                if kill_leader:
                    leader = next(node for node in replicas if node["leader"])
                    standby = next(node for node in replicas if not node["leader"])
                    stopped = leader["container"]
                    restart_policy = await docker(
                        "inspect", "--format", "{{.HostConfig.RestartPolicy.Name}}", stopped
                    )
                    # Prevent the killed process from restarting ahead of its standby.
                    await docker("update", "--restart=no", stopped)
                    await docker("kill", "--signal=KILL", stopped)
                    started = asyncio.get_running_loop().time()
                    async with asyncio.timeout(configured.scheduler.leader_ttl_seconds + 4):
                        while True:
                            value = await redis.get(configured.scheduler.leader_key)
                            if value and json.loads(value)["instance_id"] == standby["instance_id"]:
                                break
                            await asyncio.sleep(0.05)
                    elapsed = asyncio.get_running_loop().time() - started
                    print(
                        f"[OK] Killed Docker leader; original standby took over in {elapsed:.2f}s"
                    )
                async with asyncio.timeout(30):
                    while True:
                        response = await http.get(f"{api}/tasks/{identifier}")
                        response.raise_for_status()
                        task = response.json()
                        if task["status"] == "SUCCEEDED":
                            break
                        await asyncio.sleep(0.1)
                    started_at = datetime.fromisoformat(task["attempt_history"][0]["started_at"])
                    if task["attempts"] != 1 or started_at < due_at:
                        raise RuntimeError("Task ran early or executed more than once")
                    while True:
                        event = await consumer.getone()
                        if event.value.get("task_id") == identifier:
                            break
                print("[OK] Delayed task dispatched to Kafka and executed once, after its due time")
            response = await http.post(
                f"{api}/schedules",
                json={
                    "name": prefix,
                    "cron": "* * * * * */2",
                    "type": "generate_report",
                    "payload": {"data": [1, 2], "seconds": 0},
                },
            )
            response.raise_for_status()
            schedule_id = response.json()["id"]
            async with asyncio.timeout(15):
                while True:
                    page = await http.get(f"{api}/tasks", params={"schedule_id": schedule_id})
                    page.raise_for_status()
                    tasks = page.json()["items"]
                    if any(item["status"] == "SUCCEEDED" for item in tasks):
                        break
                    await asyncio.sleep(0.1)
                occurrences = [item["scheduled_for"] for item in tasks]
                if len(occurrences) != len(set(occurrences)):
                    raise RuntimeError("Cron occurrence materialized more than once")
            print("[OK] Cron run materialized and executed; occurrence identities are unique")
        finally:
            try:
                if schedule_id is not None:
                    await http.patch(f"{api}/schedules/{schedule_id}", json={"enabled": False})
            finally:
                try:
                    if stopped is not None and restart_policy is not None:
                        await docker("update", f"--restart={restart_policy}", stopped)
                        await docker(
                            "compose",
                            "up",
                            "-d",
                            "--no-deps",
                            "--wait",
                            "--wait-timeout",
                            "90",
                            "--scale",
                            "scheduler=2",
                            "scheduler",
                        )
                        print("[OK] Restored two healthy scheduler replicas")
                finally:
                    await redis.aclose()


def main() -> int:
    """Opt into actual Docker SIGKILL explicitly; normal checks leave replicas running."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kill-leader",
        action="store_true",
        help="SIGKILL the Docker leader and restore it after verification",
    )
    arguments = parser.parse_args()
    try:
        asyncio.run(check(arguments.kill_leader))
        return 0
    except Exception as exc:
        print(
            f"[FAIL] Scheduler verification: {type(exc).__name__}; "
            "inspect scheduler/API/worker logs"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
