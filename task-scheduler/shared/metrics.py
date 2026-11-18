"""Replica-safe durable metrics snapshots plus per-process worker/leader instrumentation."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from hashlib import sha256
from importlib.resources import files
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Gauge,
    GCCollector,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from shared.config import Settings

BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)
SQL = files("shared").joinpath("observability.sql").read_text()


class DurableCollector:
    """Global values are repeated by replicas; dashboards/rules deduplicate with max, never sum."""

    def __init__(self) -> None:
        self.snapshot: dict[str, Any] = {}
        self.circuits: dict[str, int] = {}

    def collect(self) -> Iterator:
        """Render a fully captured snapshot synchronously after async dependency reads complete."""
        value = self.snapshot
        yield CounterMetricFamily(
            "tasks_submitted_total",
            "Retained committed task submissions",
            value=value.get("submitted", 0),
        )
        processed = CounterMetricFamily(
            "tasks_processed_total",
            "Durable attempt outcomes, including retry attempts",
            labels=["status", "type"],
        )
        retries = CounterMetricFamily("retries_total", "Durable retry decisions", labels=["type"])
        for row in value.get("processed", []):
            processed.add_metric([row["status"], row["type"]], row["value"])
            if row["status"] == "RETRYING":
                retries.add_metric([row["type"]], row["value"])
        yield processed
        yield retries
        latency = HistogramMetricFamily(
            "task_latency_seconds",
            "Attempt execution time, excluding scheduled wait",
            labels=["type"],
        )
        for row in value.get("latency", []):
            boundaries = [(str(float(bound)), row["buckets"][f"{bound:g}"]) for bound in BUCKETS]
            latency.add_metric([row["type"]], [*boundaries, ("+Inf", row["count"])], row["sum"])
        yield latency
        queue = GaugeMetricFamily("dlq_size", "OPEN DLQ incidents by task type", labels=["type"])
        for row in value.get("dlq", []):
            queue.add_metric([row["type"]], row["value"])
        yield queue
        for name, field, help_text in (
            ("outbox_lag", "outbox_lag", "Age in seconds of the oldest unpublished outbox row"),
            ("outbox_pending", "outbox_pending", "Unpublished durable outbox rows"),
            ("running_tasks", "running", "Tasks currently RUNNING"),
            ("running_tasks_oldest_age_seconds", "running_age", "Age of the oldest RUNNING state"),
            (
                "stuck_running_tasks",
                "stuck_running",
                "RUNNING tasks older than the configured threshold",
            ),
        ):
            yield GaugeMetricFamily(name, help_text, value=value.get(field, 0))
        diagnoses = CounterMetricFamily(
            "agent_diagnoses_total",
            "Committed per-incident agent decisions, including cached diagnoses",
            labels=["category", "action"],
        )
        for row in value.get("diagnoses", []):
            diagnoses.add_metric([row["category"], row["action"]], row["value"])
        yield diagnoses
        tokens = value.get("tokens", {})
        token_metric = CounterMetricFamily(
            "agent_tokens_total",
            "Known provider-reported tokens, once per LLM turn",
            labels=["direction"],
        )
        for direction in ("input", "output"):
            token_metric.add_metric([direction], tokens.get(direction, 0))
        yield token_metric
        yield CounterMetricFamily(
            "agent_tokens_charged_total",
            "Conservatively budget-charged tokens including unknown usage",
            value=tokens.get("charged", 0),
        )
        yield CounterMetricFamily(
            "agent_cost_usd_total",
            "Budget-accounted estimated USD under configured rates",
            value=tokens.get("cost", 0),
        )
        circuit = GaugeMetricFamily(
            "circuit_breaker_open",
            "Circuit is not CLOSED, including half-open probes",
            labels=["type"],
        )
        for task_type, opened in self.circuits.items():
            circuit.add_metric([task_type], opened)
        yield circuit

    def describe(self) -> Iterator:
        """Describe fixed names without network calls during collector registration."""
        yield from self.collect()


class ServiceMetrics:
    """One registry per service instance; IDs appear in logs/headers, never metric labels."""

    def __init__(
        self,
        service: str,
        factory: async_sessionmaker[AsyncSession],
        redis: Redis,
        settings: Settings,
        *,
        registry: CollectorRegistry | None = None,
        leader: Gauge | None = None,
    ) -> None:
        self.factory, self.redis, self.settings = factory, redis, settings
        self.registry = registry if registry is not None else CollectorRegistry()
        self.collector = DurableCollector()
        self.registry.register(self.collector)
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self.inflight = Gauge(
            "worker_inflight", "Owned handlers currently executing", registry=self.registry
        )
        self.leader = (
            leader
            if leader is not None
            else Gauge(
                "scheduler_is_leader",
                "This process owns scheduler leadership",
                registry=self.registry,
            )
        )
        Gauge(
            "service_up",
            "Service metrics endpoint is available",
            ["service"],
            registry=self.registry,
        ).labels(service).set(1)
        self.lock = asyncio.Lock()

    async def render(self) -> bytes:
        """A failed/slow dependency fails the scrape instead of publishing false zeroes."""
        async with self.lock, asyncio.timeout(self.settings.metrics.scrape_timeout_seconds):
            async with self.factory.begin() as session:
                if not isinstance(session.bind, AsyncEngine):
                    raise TypeError("Metrics require an AsyncEngine-bound session")
                options = session.bind.get_execution_options()
                schema = options.get("schema_translate_map", {}).get(None)
                if schema:
                    quoted = session.bind.dialect.identifier_preparer.quote_schema(schema)
                    await session.execute(text(f"SET LOCAL search_path TO {quoted}, pg_catalog"))
                timeout = int(self.settings.metrics.scrape_timeout_seconds * 900)
                await session.execute(text(f"SET LOCAL statement_timeout='{timeout}ms'"))
                snapshot = (
                    await session.execute(
                        text(SQL), {"stuck_seconds": self.settings.metrics.stuck_running_seconds}
                    )
                ).scalar_one()
            task_types = snapshot.get("types", [])
            pipeline = self.redis.pipeline(transaction=False)
            for task_type in task_types:
                pipeline.exists(
                    self.settings.circuit.key_prefix + sha256(task_type.encode()).hexdigest()
                )
            states = await pipeline.execute() if task_types else []
            self.collector.snapshot = snapshot
            self.collector.circuits = dict(zip(task_types, map(int, states), strict=True))
            return generate_latest(self.registry)
