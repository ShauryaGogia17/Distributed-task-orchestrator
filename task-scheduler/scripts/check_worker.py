#!/usr/bin/env python3
"""Verify live API -> worker -> DB/result event -> committed Kafka input offset."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition

from shared.config import get_settings
from shared.kafka import consumer_context
from shared.redis import create_redis_client


async def check(expected_workers: int) -> None:
    """Submit one identified example report and check each durable processing boundary."""
    settings = get_settings()
    client = create_redis_client(settings.redis)
    identifier = f"worker-check-{uuid4().hex}"
    try:
        heartbeats = []
        async for key in client.scan_iter(match="worker:*"):
            value = await client.get(key)
            if value is not None:
                heartbeat = json.loads(value)
                if heartbeat.get("language") != "java" and not heartbeat["worker_id"].startswith(
                    "java-"
                ):
                    heartbeats.append(heartbeat["worker_id"])
        if len(heartbeats) < expected_workers:
            raise RuntimeError(f"expected {expected_workers} live workers, found {len(heartbeats)}")
        print(f"[OK] Redis: {len(heartbeats)} live Python worker heartbeats")
        async with consumer_context(
            "tasks.pending", "tasks.results", group_id=identifier
        ) as consumer:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{settings.api.port}", timeout=10
            ) as http:
                submitted = await http.post(
                    "/tasks",
                    json={
                        "type": "generate_report",
                        "payload": {"data": [1, 2, 3], "seconds": 0.1},
                        "priority": "normal",
                        "idempotency_key": identifier,
                    },
                )
                submitted.raise_for_status()
                task_id = submitted.json()["id"]
                dispatch = None
                result = None
                async with asyncio.timeout(
                    settings.worker.task_timeout_seconds
                    + settings.worker.result_publish_timeout_seconds
                    + 15
                ):
                    while dispatch is None or result is None:
                        record = await consumer.getone()
                        if record.value.get("task_id") != task_id:
                            continue
                        if record.topic == "tasks.pending":
                            dispatch = record
                        elif record.value.get("status") == "SUCCEEDED":
                            result = record.value
                    response = await http.get(f"/tasks/{task_id}")
                    response.raise_for_status()
                    task = response.json()
                    if (
                        task["status"] != "SUCCEEDED"
                        or task["attempts"] != 1
                        or task["result"]["sum"] != 6
                        or len(task["attempt_history"]) != 1
                    ):
                        raise RuntimeError(f"Unexpected task execution outcome for {task_id}")
                    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
                    try:
                        await admin.start()
                        partition = TopicPartition("tasks.pending", dispatch.partition)
                        while True:
                            offsets = await admin.list_consumer_group_offsets(
                                settings.worker.group_id, partitions=[partition]
                            )
                            if offsets[partition].offset >= dispatch.offset + 1:
                                break
                            await asyncio.sleep(0.05)
                    finally:
                        await admin.close()
                print(f"[OK] Task {task_id}: SUCCEEDED, one finished attempt, report sum=6")
                print(
                    "[OK] Kafka: tasks.results delivered; "
                    f"input offset {dispatch.offset + 1} committed"
                )
    finally:
        await client.aclose()


def main() -> int:
    """Print a shell-friendly live verification result without exposing credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-workers", type=int, default=1)
    args = parser.parse_args()
    if args.expected_workers < 1:
        parser.error("--expected-workers must be positive")
    try:
        asyncio.run(check(args.expected_workers))
        return 0
    except Exception as exc:
        print(f"[FAIL] Worker verification: {type(exc).__name__}; inspect API/worker logs")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
