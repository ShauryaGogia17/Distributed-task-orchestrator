#!/usr/bin/env python3
"""Verify live retry exhaustion and immediate fatal-error DLQ delivery."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx

from shared.config import get_settings
from shared.kafka import consumer_context


async def check() -> None:
    """Use refused local HTTP connections for deterministic retries, with no external dependency."""
    settings = get_settings()
    run_id = f"retry-check-{uuid4().hex}"
    async with consumer_context("tasks.dlq", group_id=run_id) as consumer:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{settings.api.port}", timeout=10
        ) as http:
            tasks = {}
            definitions = {
                "fatal": {"type": "bad_input_task", "max_retries": 5},
                "retry": {
                    "type": "http_call",
                    "max_retries": 2,
                    "payload": {"url": "http://127.0.0.1:1/", "timeout_seconds": 0.2},
                },
            }
            for name, definition in definitions.items():
                response = await http.post(
                    "/tasks", json={**definition, "idempotency_key": f"{run_id}-{name}"}
                )
                response.raise_for_status()
                tasks[response.json()["id"]] = name
            letters = {}
            async with asyncio.timeout(
                settings.worker.retry_base_seconds + 2 * settings.worker.http_timeout_seconds + 40
            ):
                while len(letters) < 2:
                    record = await consumer.getone()
                    if record.value.get("task_id") in tasks:
                        letters[record.value["task_id"]] = record.value
                for identifier, name in tasks.items():
                    while True:
                        response = await http.get(f"/tasks/{identifier}")
                        response.raise_for_status()
                        task = response.json()
                        if task["status"] == "DEAD_LETTERED":
                            break
                        await asyncio.sleep(0.05)
                    expected = 1 if name == "fatal" else 2
                    if (
                        task["attempts"] != expected
                        or len(task["attempt_history"]) != expected
                        or len(letters[identifier]["error_history"]) != expected
                    ):
                        raise RuntimeError("Unexpected retry attempt/history count")
                    reason = "fatal error" if name == "fatal" else "attempt budget exhausted"
                    if letters[identifier]["reason"] != reason:
                        raise RuntimeError("Unexpected DLQ reason")
                    print(
                        f"[OK] {name}: {expected} attempt(s), {reason}; "
                        "full history delivered to tasks.dlq"
                    )


def main() -> int:
    """Return a nonzero exit code on any missing retry/DLQ guarantee."""
    try:
        asyncio.run(check())
        return 0
    except Exception as exc:
        print(f"[FAIL] Retry verification: {type(exc).__name__}; inspect worker/API logs")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
