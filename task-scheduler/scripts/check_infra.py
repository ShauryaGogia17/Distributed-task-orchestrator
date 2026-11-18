#!/usr/bin/env python3
"""Check all infrastructure and topic partitions; exit nonzero on any failure."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

# Support the documented direct invocation without requiring an editable install.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg
from aiokafka.admin import AIOKafkaAdminClient
from pydantic import ValidationError
from redis.asyncio import Redis

from shared.config import Settings, get_settings
from shared.topics import expected_topics


class InfrastructureMismatch(Exception):
    """An infrastructure response does not match the required contract."""


@dataclass(frozen=True)
class CheckResult:
    """A sanitized, printable service status."""

    service: str
    ok: bool
    detail: str


async def check_kafka(settings: Settings) -> str:
    """Fetch broker metadata and verify every required topic and partition leader."""
    client = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id=f"{settings.kafka.client_id}-infra-check",
        request_timeout_ms=settings.kafka.request_timeout_ms,
    )
    try:
        await client.start()
        expected = expected_topics()
        metadata = await client.describe_topics(list(expected))
        actual = {topic["topic"]: topic for topic in metadata}
        problems: list[str] = []
        for name, count in expected.items():
            topic = actual.get(name)
            if topic is None or topic.get("error_code", 0):
                problems.append(f"{name}: missing or unavailable")
                continue
            partitions = topic["partitions"]
            if len(partitions) != count:
                problems.append(f"{name}: expected {count} partitions, found {len(partitions)}")
            if any(p.get("error_code", 0) or p["leader"] < 0 for p in partitions):
                problems.append(f"{name}: partition without a healthy leader")
        if problems:
            raise InfrastructureMismatch("; ".join(problems))
        summary = ", ".join(f"{name}={count}" for name, count in expected.items())
        return f"connected; all {len(expected)} topics ready ({summary})"
    finally:
        await client.close()


async def check_redis(settings: Settings) -> str:
    """Verify Redis responds to PING on the configured database."""
    async with Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        socket_connect_timeout=settings.redis.socket_timeout_seconds,
        socket_timeout=settings.redis.socket_timeout_seconds,
    ) as client:
        if not await client.ping():
            raise InfrastructureMismatch("PING did not return PONG")
    return "PING returned PONG"


async def check_postgres(settings: Settings) -> str:
    """Authenticate to the configured PostgreSQL database and execute SELECT 1."""
    pg = settings.postgres
    async with await psycopg.AsyncConnection.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.db,
        user=pg.user,
        password=pg.password.get_secret_value(),
        connect_timeout=pg.connect_timeout_seconds,
    ) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            if await cursor.fetchone() != (1,):
                raise InfrastructureMismatch("SELECT 1 returned an unexpected result")
    return "authenticated; SELECT 1 succeeded"


async def run_check(
    service: str,
    operation: Callable[[], Awaitable[str]],
    deadline_seconds: float,
) -> CheckResult:
    """Bound each check and suppress exception messages that may contain secrets."""
    try:
        async with asyncio.timeout(deadline_seconds):
            detail = await operation()
        return CheckResult(service, True, detail)
    except InfrastructureMismatch as exc:
        return CheckResult(service, False, str(exc))
    except Exception as exc:
        return CheckResult(service, False, f"{type(exc).__name__}; check endpoint and service logs")


async def check_infrastructure(settings: Settings) -> list[CheckResult]:
    """Run independent checks concurrently so one outage cannot hide other results."""
    timeout = settings.infra_check_timeout_seconds
    return list(
        await asyncio.gather(
            run_check("Kafka", lambda: check_kafka(settings), timeout),
            run_check("Redis", lambda: check_redis(settings), timeout),
            run_check("Postgres", lambda: check_postgres(settings), timeout),
        )
    )


def main() -> int:
    """Print OK/FAIL per service and return a shell-friendly aggregate exit code."""
    try:
        settings = get_settings()
    except ValidationError as exc:
        fields = ", ".join(".".join(map(str, error["loc"])) for error in exc.errors())
        print(f"[FAIL] Configuration: invalid or missing fields: {fields}; check .env")
        return 1
    results = asyncio.run(check_infrastructure(settings))
    for result in results:
        status = "OK" if result.ok else "FAIL"
        if sys.stdout.isatty():
            color = "\033[32m" if result.ok else "\033[31m"
            status = f"{color}{status}\033[0m"
        print(f"[{status}] {result.service}: {result.detail}")
    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
