#!/usr/bin/env python3
"""Run explicit fault scenarios only against a labelled, isolated task-scheduler test project."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, TypeVar
from uuid import UUID, uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import func, select

from services.agent.core import AgentCore
from services.agent.llm.base import Completion, LLMClient
from shared.config import AgentSettings, get_settings
from shared.db import create_engine, create_session_factory, database_now
from shared.kafka import producer_context
from shared.models import TaskMessage, TaskStatus
from shared.redis import create_redis_client
from shared.tables import AgentAudit, DeadLetter, Outbox, Schedule, Task, TaskAttempt
from shared.tracing import headers_for

T = TypeVar("T")


class ChaosSettings(BaseSettings):
    """No defaults can accidentally select the ordinary development/production project."""

    model_config = SettingsConfigDict(env_prefix="CHAOS_", extra="ignore")
    project: str = Field(pattern=r"^task-scheduler-test-[a-z0-9-]+$")
    api_url: str = "http://api:8000"
    timeout_seconds: float = Field(default=120, gt=0)
    kafka_outage_seconds: int = Field(default=30, ge=30)
    evidence_file: Path = Path("artifacts/chaos/evidence.jsonl")


async def wait_for(check: Callable[[], Awaitable[T | None]], seconds: float, description: str) -> T:
    """Poll a bounded condition; timeout names the missing invariant in FAIL evidence."""
    try:
        async with asyncio.timeout(seconds):
            while True:
                result = await check()
                if result is not None:
                    return result
                await asyncio.sleep(0.2)
    except TimeoutError as exc:
        raise TimeoutError(f"Timed out waiting for {description} after {seconds:g}s") from exc


class DockerFaults:
    """Resolve and revalidate Compose ownership before each Docker mutation."""

    SERVICES = {"worker-a", "worker-b", "scheduler-a", "scheduler-b", "kafka", "redis"}

    def __init__(self, project: str) -> None:
        if not re.fullmatch(r"task-scheduler-test-[a-z0-9-]+", project):
            raise ValueError("Faults require an isolated test project")
        self.project = project

    async def command(self, *arguments: str) -> str:
        """Use argv rather than shell interpolation; retain no inspect secrets in logs."""
        process = await asyncio.create_subprocess_exec(
            "docker", *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 60)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        if process.returncode:
            raise RuntimeError(f"Docker {arguments[0]} failed: {stderr.decode()[:500]}")
        return stdout.decode()

    async def container(self, service: str) -> dict[str, Any]:
        """Accept exactly one owned instance of a known test service, never an arbitrary ID."""
        if service not in self.SERVICES:
            raise ValueError("Service is not a permitted fault target")
        ids = (
            await self.command(
                "ps",
                "-aq",
                "--filter",
                f"label=com.docker.compose.project={self.project}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--filter",
                "label=io.task-scheduler.test-stack=true",
            )
        ).split()
        if len(ids) != 1:
            raise RuntimeError(f"Expected one labelled {service}, found {len(ids)}")
        info = json.loads(await self.command("inspect", ids[0]))[0]
        labels = info["Config"]["Labels"]
        if (
            labels.get("com.docker.compose.project") != self.project
            or labels.get("io.task-scheduler.test-stack") != "true"
        ):
            raise RuntimeError("Refusing to mutate a container outside the test project")
        return info

    async def mutate(self, service: str, operation: str, *arguments: str) -> None:
        """Recheck the target immediately before issuing a fault or recovery action."""
        info = await self.container(service)
        await self.command(operation, *arguments, info["Id"])

    @asynccontextmanager
    async def killed(self, service: str) -> AsyncIterator[None]:
        """Keep the victim down for failover; always restore its policy and start it."""
        info = await self.container(service)
        policy = info["HostConfig"]["RestartPolicy"]["Name"] or "no"
        await self.mutate(service, "update", "--restart=no")
        try:
            await self.mutate(service, "kill", "--signal=KILL")
            yield
        finally:
            await self.mutate(service, "update", f"--restart={policy}")
            await self.mutate(service, "start")


class FaultLLM(LLMClient):
    """Deterministic provider faults injected through the real LLM interface, without API egress."""

    def __init__(self, mode: str) -> None:
        self.mode, self.calls = mode, 0

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], schema: dict[str, Any]
    ) -> Completion:
        """Timeout under the real agent deadline, or return invalid JSON through the repair turn."""
        self.calls += 1
        if self.mode == "timeout":
            await asyncio.sleep(10)
        return Completion(content=[{"type": "text", "text": "{invalid JSON"}])


class ChaosSuite:
    """Exercise real containers, Kafka, Redis, SQL transactions, and agent policy boundaries."""

    def __init__(self, options: ChaosSettings) -> None:
        self.options, self.settings = options, get_settings()
        self.docker = DockerFaults(options.project)
        self.engine = create_engine(self.settings.postgres)
        self.factory = create_session_factory(self.engine)
        self.redis = create_redis_client(self.settings.redis)
        self.http = httpx.AsyncClient(base_url=options.api_url, timeout=15)

    async def close(self) -> None:
        """Release owned clients even after a failed invariant."""
        await self.http.aclose()
        await self.redis.aclose()
        await self.engine.dispose()

    async def submit(
        self,
        kind: str = "generate_report",
        payload: dict[str, Any] | None = None,
        *,
        key: str | None = None,
    ) -> dict[str, Any]:
        """Submit uniquely identified synthetic work through the public transactional API."""
        response = await self.http.post(
            "/tasks",
            json={
                "type": kind,
                "payload": payload or {},
                "max_retries": 5,
                "idempotency_key": key or f"chaos-{uuid4()}",
                "priority": "normal",
            },
        )
        response.raise_for_status()
        return response.json()

    async def successful(self, identifier: str) -> dict[str, Any]:
        """Require success, failing immediately for an unexpected terminal state."""

        async def read() -> dict[str, Any] | None:
            response = await self.http.get(f"/tasks/{identifier}")
            response.raise_for_status()
            result = response.json()
            if result["status"] in {"FAILED", "DEAD_LETTERED", "ESCALATED", "CANCELLED"}:
                raise AssertionError(f"Task {identifier} ended {result['status']}")
            return result if result["status"] == "SUCCEEDED" else None

        return await wait_for(read, self.options.timeout_seconds, f"task {identifier} success")

    async def worker_crash(self) -> dict[str, Any]:
        """Kill an owned RUNNING attempt; prove one durable success and idempotent redelivery."""
        key = f"chaos-crash-{uuid4()}"
        created = await self.submit("slow_task", {"seconds": 3}, key=key)
        identifier = UUID(created["id"])

        async def running() -> str | None:
            async with self.factory() as session:
                row = await session.get(Task, identifier)
                if row is None or row.status != TaskStatus.RUNNING:
                    return None
                return await session.scalar(
                    select(TaskAttempt.worker_id).where(
                        TaskAttempt.task_id == identifier, TaskAttempt.attempt_no == row.attempts
                    )
                )

        owner = await wait_for(running, 30, "RUNNING worker owner")
        async with self.docker.killed(owner):
            completed = await self.successful(str(identifier))
        assert completed["attempts"] >= 2, "Crash did not cause an attempt to be reclaimed"
        repeated = await self.submit("slow_task", {"seconds": 0}, key=key)
        assert repeated["id"] == str(identifier), "HTTP idempotency created a second task"
        async with self.factory() as session:
            row = await session.get(Task, identifier)
            assert row is not None
            message = TaskMessage(
                task_id=row.id,
                type=row.type,
                payload=row.payload,
                scheduled_at=row.scheduled_at,
                created_at=row.created_at,
                max_retries=row.max_retries,
            )
        async with producer_context(self.settings.kafka) as producer:
            from shared.priority import message_key, pending_partition

            kafka_key = message_key(identifier, 5)
            replay = await producer.send_and_wait(
                "tasks.pending",
                key=kafka_key,
                value=message,
                partition=pending_partition(kafka_key),
                headers=headers_for(identifier, UUID(created["trace_id"])),
            )
        admin = AIOKafkaAdminClient(bootstrap_servers=self.settings.kafka.bootstrap_servers)
        try:
            await admin.start()

            async def acknowledged() -> bool | None:
                offsets = await admin.list_consumer_group_offsets(self.settings.worker.group_id)
                offset = offsets.get(TopicPartition("tasks.pending", replay.partition))
                return True if offset and offset.offset > replay.offset else None

            await wait_for(acknowledged, 45, "redelivery offset committed")
        finally:
            await admin.close()
        async with self.factory() as session:
            row = await session.get(Task, identifier)
            successes = list(
                await session.scalars(
                    select(Outbox).where(
                        Outbox.topic == "tasks.results", Outbox.key == str(identifier)
                    )
                )
            )
        count = sum(event.payload.get("status") == "SUCCEEDED" for event in successes)
        assert row is not None and row.attempts == completed["attempts"] and count == 1
        return {
            "task_id": str(identifier),
            "killed_worker": owner,
            "attempts": row.attempts,
            "durable_successes": count,
            "replay_offset_committed": replay.offset + 1,
            "same_idempotency_task": repeated["id"],
        }

    async def kafka_outage(self) -> dict[str, Any]:
        """Hold Kafka stopped for at least thirty seconds while the API commits new outboxes."""
        identifiers: list[str] = []
        await self.docker.mutate("kafka", "stop", "--time=10")
        stopped = time.monotonic()
        try:
            for _ in range(12):
                identifiers.append((await self.submit(payload={"seconds": 0}))["id"])
            async with self.factory() as session:
                durable = await session.scalar(
                    select(func.count())
                    .select_from(Task)
                    .where(Task.id.in_([UUID(v) for v in identifiers]))
                )
                pending = await session.scalar(
                    select(func.count())
                    .select_from(Outbox)
                    .where(
                        Outbox.published_at.is_(None),
                        Outbox.payload["task_id"].astext.in_(identifiers),
                    )
                )
            assert durable == pending == len(identifiers), (
                "Outage submissions were not retained durably"
            )
            await asyncio.sleep(
                max(0, self.options.kafka_outage_seconds - (time.monotonic() - stopped))
            )
        finally:
            outage = time.monotonic() - stopped
            await self.docker.mutate("kafka", "start")
        for identifier in identifiers:
            await self.successful(identifier)
        async with self.factory() as session:
            pending_after = await session.scalar(
                select(func.count())
                .select_from(Outbox)
                .where(
                    Outbox.published_at.is_(None), Outbox.payload["task_id"].astext.in_(identifiers)
                )
            )
        assert pending_after == 0 and outage >= 30
        return {
            "broker_stopped_seconds": round(outage, 3),
            "submitted": len(identifiers),
            "durable_tasks_during_outage": durable,
            "pending_outboxes_during_outage": pending,
            "succeeded": len(identifiers),
            "pending_after_recovery": pending_after,
            "task_ids": identifiers,
        }

    async def scheduler_failover(self) -> dict[str, Any]:
        """Kill the Redis-elected leader and verify one occurrence after a standby takes over."""

        async def leader() -> str | None:
            raw = await self.redis.get(self.settings.scheduler.leader_key)
            return json.loads(raw)["instance_id"] if raw else None

        old = await wait_for(leader, 30, "scheduler leader")
        started = time.monotonic()
        async with self.docker.killed(old):

            async def successor() -> str | None:
                value = await leader()
                return value if value and value != old else None

            new = await wait_for(successor, 15, "standby leadership")
            elapsed = time.monotonic() - started
        response = await self.http.post(
            "/schedules",
            json={
                "type": "generate_report",
                "payload": {"seconds": 0},
                "cron": "0 0 1 1 *",
                "missed_run_policy": "fire_once",
            },
        )
        response.raise_for_status()
        schedule_id = UUID(response.json()["id"])
        async with self.factory.begin() as session:
            definition = await session.get(Schedule, schedule_id)
            assert definition is not None
            definition.next_run_at = await database_now(session) + __import__("datetime").timedelta(
                seconds=2
            )
            occurrence = definition.next_run_at

        async def materialized() -> Task | None:
            async with self.factory() as session:
                return await session.scalar(select(Task).where(Task.schedule_id == schedule_id))

        row = await wait_for(materialized, 30, "materialized recurrence")
        await self.successful(str(row.id))
        # Both schedulers are back online; replay opportunities cannot create another identity.
        await asyncio.sleep(2)
        async with self.factory() as session:
            rows = list(await session.scalars(select(Task).where(Task.schedule_id == schedule_id)))
            assert len(rows) == 1 and rows[0].scheduled_for == occurrence
            events = list(
                await session.scalars(
                    select(Outbox).where(
                        Outbox.payload["task_id"].astext == str(row.id),
                        Outbox.topic == "tasks.pending",
                    )
                )
            )
            definition = await session.get(Schedule, schedule_id)
            assert definition is not None
            definition.enabled = False
            await session.commit()
        assert len(events) == 1
        return {
            "killed_leader": old,
            "successor": new,
            "failover_seconds": round(elapsed, 3),
            "schedule_id": str(schedule_id),
            "occurrence": occurrence.isoformat(),
            "task_id": str(row.id),
            "materialized_runs": len(rows),
            "pending_events": len(events),
        }

    async def agent_fault(self, mode: str) -> dict[str, Any]:
        """Drive a real incident through collection, guarded diagnosis, repair, and escalation."""
        created = await self.submit(f"chaos.agent_{mode}")
        identifier = UUID(created["id"])

        async def received() -> DeadLetter | None:
            async with self.factory() as session:
                return await session.scalar(
                    select(DeadLetter).where(
                        DeadLetter.task_id == identifier, DeadLetter.received_at.is_not(None)
                    )
                )

        incident = await wait_for(received, 45, "persisted DLQ incident")
        model = FaultLLM(mode)
        settings = self.settings.model_copy(
            update={
                "agent": AgentSettings(
                    _env_file=None,
                    enabled=True,
                    cluster_debounce_seconds=0,
                    diagnosis_timeout_seconds=1,
                    lease_seconds=20,
                    budget_key_prefix=f"chaos:budget:{uuid4()}:",
                )
            }
        )
        agent = AgentCore(self.factory, self.redis, settings, lambda: model)
        await agent.tick()
        async with self.factory() as session:
            row = await session.get(Task, identifier)
            stored = await session.get(DeadLetter, incident.id)
            audits = list(
                await session.scalars(
                    select(AgentAudit).where(AgentAudit.dead_letter_id == incident.id)
                )
            )
        assert row is not None and stored is not None
        assert row.status == TaskStatus.ESCALATED and stored.ai_diagnosis["action"] == "ESCALATE"
        assert row.generation == 0 and row.ai_requeues == 0
        expected = 1 if mode == "timeout" else 2
        assert model.calls == expected, f"Expected {expected} bounded calls, saw {model.calls}"
        assert any(event.event == "ACTION_TAKEN" for event in audits)
        return {
            "task_id": str(identifier),
            "status": row.status.value,
            "action": stored.ai_diagnosis["action"],
            "llm_calls": model.calls,
            "repair_attempts": model.calls - 1,
            "generation": row.generation,
            "reason": stored.ai_diagnosis["reasoning"],
        }

    async def redis_restart(self) -> dict[str, Any]:
        """Restart the actual Redis container, then require both live workers and resumed work."""

        async def workers() -> list[str] | None:
            live = [
                name
                for name in ("worker-a", "worker-b")
                if await self.redis.exists(f"worker:{name}")
            ]
            return live if len(live) == 2 else None

        before = await wait_for(workers, 30, "initial worker heartbeats")
        info = await self.docker.container("redis")
        started_before = info["State"]["StartedAt"]
        await self.docker.mutate("redis", "restart", "--time=10")
        info = await self.docker.container("redis")
        assert info["State"]["StartedAt"] != started_before

        async def recovered() -> list[str] | None:
            try:
                if not await self.redis.ping():
                    return None
                return await workers()
            except Exception:
                return None

        after = await wait_for(recovered, 60, "Redis and worker heartbeats after restart")
        submitted = [await self.submit(payload={"seconds": 0.1}) for _ in range(4)]
        completed = [await self.successful(item["id"]) for item in submitted]
        return {
            "redis_started_before": started_before,
            "redis_started_after": info["State"]["StartedAt"],
            "workers_before": before,
            "workers_after": after,
            "resumed_tasks": [item["id"] for item in completed],
            "succeeded": len(completed),
        }

    async def run_case(self, case: str) -> dict[str, Any]:
        """Print and persist one PASS/FAIL record with concrete invariant evidence."""
        started = time.monotonic()
        try:
            if case.startswith("agent-"):
                evidence = await self.agent_fault(case.removeprefix("agent-"))
            else:
                operation = {
                    "worker-crash": self.worker_crash,
                    "kafka-outage": self.kafka_outage,
                    "scheduler-failover": self.scheduler_failover,
                    "redis-restart": self.redis_restart,
                }[case]
                evidence = await operation()
            record = {
                "scenario": case,
                "result": "PASS",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "evidence": evidence,
            }
        except BaseException as exc:
            record = {
                "scenario": case,
                "result": "FAIL",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
            self.report(record)
            raise
        self.report(record)
        return record

    def report(self, record: dict[str, Any]) -> None:
        """Make evidence readable in a terminal and retain machine-readable CI artifacts."""
        self.options.evidence_file.parent.mkdir(parents=True, exist_ok=True)
        with self.options.evidence_file.open("a") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        print(
            f"{record['result']} [{record['scenario']}] "
            + json.dumps(record.get("evidence", {"error": record.get("error")}), sort_keys=True),
            flush=True,
        )


CASES = (
    "worker-crash",
    "kafka-outage",
    "scheduler-failover",
    "agent-timeout",
    "agent-invalid",
    "redis-restart",
)


async def execute(cases: list[str]) -> None:
    """Run requested cases sequentially so faults do not mask one another."""
    suite = ChaosSuite(ChaosSettings())
    try:
        for case in cases:
            await suite.run_case(case)
    finally:
        await suite.close()


def main() -> int:
    """Direct use requires an owned test environment; test_stack.py supplies it safely."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", choices=(*CASES, "all"), default="all", nargs="?")
    args = parser.parse_args()
    try:
        asyncio.run(execute(list(CASES) if args.case == "all" else [args.case]))
        return 0
    except Exception as exc:
        print(f"FAIL [chaos] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
