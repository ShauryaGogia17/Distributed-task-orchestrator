#!/usr/bin/env python3
"""Verify live scrape discovery, provisioned Grafana, metric queries, and Kafka task traces."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from uuid import UUID, uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx

from shared.config import get_settings
from shared.kafka import consumer_context


async def check() -> None:
    """Submit an isolated task and validate observability across the real running stack."""
    settings = get_settings()
    prom = f"http://127.0.0.1:{settings.observability.prometheus_port}"
    grafana = f"http://127.0.0.1:{settings.observability.grafana_port}"
    api = f"http://127.0.0.1:{settings.api.port}"
    trace = uuid4().hex
    async with httpx.AsyncClient(timeout=10) as http:
        async with asyncio.timeout(60):
            while True:
                response = await http.get(prom + "/api/v1/targets")
                response.raise_for_status()
                targets = response.json()["data"]["activeTargets"]
                jobs = {t["labels"]["job"] for t in targets}
                if {"api", "worker", "scheduler", "dlq", "agent"} <= jobs and all(
                    t["health"] == "up" for t in targets
                ):
                    break
                await asyncio.sleep(0.5)
        counts = {job: sum(t["labels"]["job"] == job for t in targets) for job in sorted(jobs)}
        print("[OK] Prometheus targets healthy:", counts)

        async def query(expr: str) -> list:
            response = await http.get(prom + "/api/v1/query", params={"query": expr})
            response.raise_for_status()
            body = response.json()
            if body["status"] != "success":
                raise RuntimeError("PromQL failed: " + expr)
            return body["data"]["result"]

        leader = await query('max(scheduler_is_leader{job="scheduler"})')
        if not leader or float(leader[0]["value"][1]) != 1:
            raise RuntimeError("No observable scheduler leader")
        print(
            "[OK] Exactly one observable leader:",
            (await query('sum(scheduler_is_leader{job="scheduler"})'))[0]["value"][1],
        )
        response = await http.get(grafana + "/api/dashboards/uid/task-scheduler")
        response.raise_for_status()
        dashboard = response.json()["dashboard"]
        if len(dashboard["panels"]) < 5:
            raise RuntimeError("Dashboard panels missing")
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                await query(target["expr"])
        response = await http.get(grafana + "/api/datasources/uid/prometheus/health")
        response.raise_for_status()
        if response.json()["status"] != "OK":
            raise RuntimeError("Grafana datasource failed")
        print("[OK] Provisioned Grafana dashboard, datasource, and all panel queries")
        response = await http.get(prom + "/api/v1/rules", params={"type": "alert"})
        response.raise_for_status()
        rules = [rule for group in response.json()["data"]["groups"] for rule in group["rules"]]
        if {r["name"] for r in rules} != {
            "DLQGrowing",
            "TasksStuckRunning",
            "SchedulerLeaderAbsent",
        }:
            raise RuntimeError("Alert rules missing")
        print("[OK] All three alert rules loaded")
        # Dedicated consumer reads pending and results; no application group offsets are changed.
        async with consumer_context(
            "tasks.pending", "tasks.results", group_id=f"observability-check-{uuid4()}"
        ) as consumer:
            async with asyncio.timeout(15):
                while not consumer.assignment():
                    await consumer.getmany(timeout_ms=100, max_records=1)
            await consumer.seek_to_end()
            response = await http.post(
                api + "/tasks",
                headers={"X-Trace-ID": trace},
                json={
                    "type": "generate_report",
                    "payload": {"seconds": 0.05},
                    "idempotency_key": f"observability-{trace}",
                },
            )
            response.raise_for_status()
            identifier = response.json()["id"]
            if (
                UUID(response.json()["trace_id"]).hex != trace
                or response.headers["x-trace-id"] != trace
            ):
                raise RuntimeError("HTTP trace mismatch")
            seen = set()
            async with asyncio.timeout(45):
                while len(seen) < 2:
                    record = await consumer.getone()
                    if record.value.get("task_id") != identifier:
                        continue
                    headers = dict(record.headers)
                    if (
                        headers.get("trace_id") != trace.encode()
                        or headers.get("task_id") != identifier.encode()
                    ):
                        raise RuntimeError("Kafka trace mismatch")
                    seen.add(record.topic)
                    if (
                        record.topic == "tasks.results"
                        and record.value.get("duration_seconds") is None
                    ):
                        raise RuntimeError("Execution latency missing")
            print(
                f"[OK] HTTP → Kafka → worker → result correlation: task_id={identifier} "
                f"trace_id={trace}"
            )
        for metric in (
            "tasks_submitted_total",
            "tasks_processed_total",
            "task_latency_seconds_count",
            "retries_total",
            "dlq_size",
            "outbox_lag",
            "worker_inflight",
            "scheduler_is_leader",
            "agent_diagnoses_total",
            "agent_tokens_total",
            "circuit_breaker_open",
        ):
            # Dynamic label families are absent until the first matching event/type exists.
            await query(metric)
        print("[OK] All metric queries accepted (event families appear after matching activity)")


def main() -> int:
    """Exit nonzero on any dependency, provisioning, or correlation failure."""
    try:
        asyncio.run(check())
        return 0
    except Exception as exc:
        print(f"[FAIL] Observability: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
