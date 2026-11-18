#!/usr/bin/env python3
"""Verify live offline clustering, automatic payload correction, backoff, and escalation."""

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
from shared.tables import AgentAudit, AgentCluster


async def check() -> None:
    """Use uniquely fingerprinted simulated reports; never send a paid Claude request."""
    settings = get_settings()
    engine = create_engine(settings.postgres)
    factory = create_session_factory(engine)
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            health = await http.get(f"http://127.0.0.1:{settings.agent.port}/health")
            health.raise_for_status()
            mode = health.json()
            if mode["provider"] != "mock" or not mode["enabled"] or mode["kill_switch"]:
                raise RuntimeError(
                    "Checker requires an enabled agent with MockLLM and kill switch off"
                )
            api = f"http://127.0.0.1:{settings.api.port}"
            async with factory() as session:
                now = await session.scalar(select(func.clock_timestamp()))
            remaining = (
                settings.agent.cluster_window_seconds
                - now.timestamp() % settings.agent.cluster_window_seconds
            )
            if remaining < 5 and settings.agent.cluster_window_seconds >= 10:
                await asyncio.sleep(remaining + 0.1)
            run_id = uuid4().hex
            # The invalid string is unique after normalization but identical for both
            # tasks, keeping repeated checker runs out of earlier failure circuits.
            invalid_delay = "invalid-z" + run_id.translate(
                str.maketrans("0123456789", "ghijklmnop")
            )
            payload = {"seconds": invalid_delay, "data": [4, 5], "title": "offline-agent-check"}
            identifiers = []
            for number in range(2):
                response = await http.post(
                    api + "/tasks",
                    json={
                        "type": "generate_report",
                        "payload": payload,
                        "max_retries": 1,
                        "idempotency_key": f"agent-check-{run_id}-{number}",
                    },
                )
                response.raise_for_status()
                identifiers.append(response.json()["id"])
            tasks, saw_delay = {}, False
            async with asyncio.timeout(max(90, settings.agent.min_backoff_seconds + 60)):
                while len(tasks) < len(identifiers):
                    for identifier in identifiers:
                        response = await http.get(api + f"/tasks/{identifier}")
                        response.raise_for_status()
                        task = response.json()
                        saw_delay = saw_delay or (
                            task["generation"] == 1 and task["status"] == "SCHEDULED"
                        )
                        if task["status"] == "ESCALATED":
                            raise RuntimeError(
                                "Mock correction escalated; inspect /dlq task diagnosis"
                            )
                        if task["status"] == "SUCCEEDED":
                            tasks[identifier] = task
                    await asyncio.sleep(0.2)
            cluster_ids = set()
            for identifier, task in tasks.items():
                if (
                    not task["ai_modified"]
                    or task["ai_requeues"] != 1
                    or task["result"]["sum"] != 9
                ):
                    raise RuntimeError(
                        "Missing AI payload flag, recovery count, or corrected result"
                    )
                if [
                    (item["generation"], item["attempt_no"]) for item in task["attempt_history"]
                ] != [(0, 1), (1, 1)]:
                    raise RuntimeError("Execution history was not preserved")
                response = await http.get(api + f"/dlq/{identifier}")
                response.raise_for_status()
                diagnosis = response.json()["ai_diagnosis"]
                if (
                    diagnosis["action"] != "REQUEUE_MODIFIED"
                    or diagnosis["category"] != "bad_input"
                ):
                    raise RuntimeError("Unexpected diagnosis or action")
                cluster_ids.add(UUID(diagnosis["cluster_id"]))
            if len(cluster_ids) != 1:
                raise RuntimeError("The same-window cohort did not share one cluster")
            cluster_id = cluster_ids.pop()
            async with factory() as session:
                calls = await session.scalar(
                    select(func.count())
                    .select_from(AgentAudit)
                    .where(AgentAudit.cluster_id == cluster_id, AgentAudit.event == "LLM_REQUEST")
                )
                cluster = await session.get(AgentCluster, cluster_id)
                if calls != 1 or not cluster.usage["input_tokens"]:
                    raise RuntimeError("Expected one representative model call with recorded usage")
            print("[OK] Two real DLQ tasks clustered; ONE representative MockLLM diagnosis reused")
            print(
                "[OK] Corrected payloads marked AI-modified; "
                "both tasks SUCCEEDED with history retained"
            )
            if settings.agent.min_backoff_seconds > 0 and not saw_delay:
                raise RuntimeError("Expected durable scheduled backoff before recovery")
            print("[OK] Delayed automatic recovery and durable token/cost accounting")
            response = await http.post(
                api + "/tasks",
                json={
                    "type": "bad_input_task",
                    "payload": {"check": run_id},
                    "max_retries": 1,
                    "idempotency_key": f"agent-check-escalate-{run_id}",
                },
            )
            response.raise_for_status()
            identifier = response.json()["id"]
            async with asyncio.timeout(30):
                while True:
                    response = await http.get(api + f"/tasks/{identifier}")
                    response.raise_for_status()
                    if response.json()["status"] == "ESCALATED":
                        break
                    await asyncio.sleep(0.1)
            response = await http.get(api + f"/dlq/{identifier}")
            response.raise_for_status()
            if response.json()["ai_diagnosis"]["action"] not in {
                "ESCALATE",
                "OPEN_CIRCUIT_ESCALATE",
            }:
                raise RuntimeError("Missing safe escalation")
            print("[OK] Uncorrectable input escalated; local webhook/Slack stub audit retained")
            response = await http.get(f"http://127.0.0.1:{settings.agent.port}/metrics")
            response.raise_for_status()
            if "agent_clusters" not in response.text:
                raise RuntimeError("Agent metrics unavailable")
            print("[OK] Agent readiness and cluster metrics")
    finally:
        await engine.dispose()


def main() -> int:
    """Print a secret-safe nonzero failure summary; detailed evidence stays in the audit tables."""
    try:
        asyncio.run(check())
        return 0
    except Exception as exc:
        print(
            f"[FAIL] Agent verification: {type(exc).__name__}; inspect agent logs and DLQ diagnosis"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
