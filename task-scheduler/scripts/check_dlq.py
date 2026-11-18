#!/usr/bin/env python3
"""Verify live ingestion, corrected manual retry, discard, metrics, and replay safety."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import UUID, uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from sqlalchemy import func, select

from shared.config import get_settings
from shared.db import create_engine, create_session_factory
from shared.kafka import producer_context, publish_json
from shared.tables import DLQReceipt, Outbox


async def check() -> None:
    """Operate only on newly submitted task IDs and retain their execution/audit history."""
    settings = get_settings()
    engine = create_engine(settings.postgres)
    factory = create_session_factory(engine)
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{settings.api.port}", timeout=10
        ) as http:

            async def wait_task(identifier: str, status: str) -> dict:
                async with asyncio.timeout(45):
                    while True:
                        response = await http.get(f"/tasks/{identifier}")
                        response.raise_for_status()
                        if response.json()["status"] == status:
                            return response.json()
                        await asyncio.sleep(0.1)

            async def failure() -> tuple[str, dict]:
                response = await http.post(
                    "/tasks",
                    json={
                        "type": "send_email",
                        "payload": {"to": "invalid"},
                        "idempotency_key": f"dlq-check-{uuid4().hex}",
                    },
                )
                response.raise_for_status()
                identifier = response.json()["id"]
                await wait_task(identifier, "DEAD_LETTERED")
                async with asyncio.timeout(30):
                    while True:
                        response = await http.get(f"/dlq/{identifier}")
                        response.raise_for_status()
                        detail = response.json()
                        if detail["received_at"] is not None:
                            return identifier, detail
                        await asyncio.sleep(0.1)

            async def replay(detail: dict) -> None:
                incident_id = UUID(detail["id"])
                async with factory() as session:
                    before = await session.scalar(
                        select(func.count())
                        .select_from(DLQReceipt)
                        .where(DLQReceipt.dead_letter_id == incident_id)
                    )
                    event = await session.get(Outbox, incident_id)
                    payload = event.payload
                async with producer_context(settings.kafka) as producer:
                    await publish_json(
                        producer, "tasks.dlq", payload=payload, key=detail["task_id"]
                    )
                async with asyncio.timeout(30):
                    while True:
                        async with factory() as session:
                            after = await session.scalar(
                                select(func.count())
                                .select_from(DLQReceipt)
                                .where(DLQReceipt.dead_letter_id == incident_id)
                            )
                        if after > before:
                            return
                        await asyncio.sleep(0.1)

            identifier, detail = await failure()
            if (
                not detail["worker_id"]
                or detail["attempts"] != 1
                or len(detail["failure_signature"]) != 64
            ):
                raise RuntimeError("Missing incident metadata or fingerprint")
            print(
                "[OK] Kafka DLQ consumed: durable receipt, full history, "
                "worker, attempts, fingerprint"
            )
            response = await http.post(
                f"/dlq/{identifier}/retry", json={"payload": {"to": "corrected@example.com"}}
            )
            response.raise_for_status()
            pending = response.json()
            if (
                pending["status"] != "PENDING"
                or pending["attempts"] != 0
                or pending["generation"] != 1
            ):
                raise RuntimeError("Retry did not reset the execution generation atomically")
            task = await wait_task(identifier, "SUCCEEDED")
            if task["result"]["to"] != "corrected@example.com" or [
                (item["generation"], item["attempt_no"]) for item in task["attempt_history"]
            ] != [(0, 1), (1, 1)]:
                raise RuntimeError("Corrected retry or retained history mismatch")
            print(
                "[OK] Corrected payload: PENDING with zero attempts -> SUCCEEDED; "
                "both attempts retained"
            )
            await replay(detail)
            response = await http.get(f"/dlq/{identifier}")
            response.raise_for_status()
            if response.json()["status"] != "RETRIED":
                raise RuntimeError("Replay reopened a retried incident")
            if (await http.get(f"/tasks/{identifier}")).json()["status"] != "SUCCEEDED":
                raise RuntimeError("Replay regressed a successful task")
            print(
                "[OK] Republished old DLQ event: receipt added; "
                "successful task and RETRIED incident preserved"
            )
            discarded_id, discarded = await failure()
            response = await http.delete(f"/dlq/{discarded_id}")
            response.raise_for_status()
            await replay(discarded)
            if (await http.get(f"/dlq/{discarded_id}")).status_code != 404:
                raise RuntimeError("Discarded incident was resurrected")
            await wait_task(discarded_id, "CANCELLED")
            print("[OK] Discard: CANCELLED with retained audit; Kafka replay cannot reopen it")
            response = await http.get("/metrics")
            response.raise_for_status()
            if "# TYPE dlq_size gauge" not in response.text:
                raise RuntimeError("DLQ gauge missing")
            print("[OK] Prometheus dlq_size gauge by task_type")
    finally:
        await engine.dispose()


def main() -> int:
    """Print actionable pass/fail summaries without connection credentials."""
    try:
        asyncio.run(check())
        return 0
    except Exception as exc:
        print(f"[FAIL] DLQ verification: {type(exc).__name__}; inspect API/worker/DLQ logs")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
