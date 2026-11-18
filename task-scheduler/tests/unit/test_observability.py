"""Correlation normalization, async log scoping, and replica-safe metric exposition."""

import asyncio
import json
from uuid import UUID, uuid4

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.parser import text_string_to_metric_families

from shared.logging import log_context
from shared.metrics import BUCKETS, DurableCollector
from shared.tracing import headers_for, record_trace, request_trace, trace_id
from tests.unit.test_logging import make_logger


def test_trace_headers_accept_uuid_and_traceparent() -> None:
    """HTTP and Kafka agree on lowercase nonzero 32-hex traces, including legacy fallback."""
    identifier = uuid4()
    assert request_trace({"x-trace-id": str(identifier)}) == identifier.hex
    assert (
        request_trace({"traceparent": f"00-{identifier.hex}-1234567890abcdef-01"}) == identifier.hex
    )
    assert record_trace(headers_for(uuid4(), identifier)) == identifier.hex
    for invalid in ("garbage", "0" * 32, None, b"\xff"):
        assert UUID(trace_id(invalid)).int > 0
    assert UUID(record_trace(None)).int > 0


async def test_traces_are_scoped_per_coroutine() -> None:
    """Worker/relay logs cannot accidentally reuse another concurrent task's trace."""
    logger, stream = make_logger()

    async def emit(identifier: UUID) -> None:
        with log_context(task_id=identifier, trace_id=identifier):
            await asyncio.sleep(0)
            logger.info("correlated")

    ids = [uuid4(), uuid4()]
    await asyncio.gather(*(emit(identifier) for identifier in ids))
    logger.info("outside")
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert {row["trace_id"] for row in rows[:2]} == {identifier.hex for identifier in ids}
    assert rows[-1]["trace_id"] is None


def test_histogram_uses_cumulative_buckets_and_no_identifier_labels() -> None:
    """p95 inputs must be valid cumulative buckets and token totals count model turns."""
    collector = DurableCollector()
    collector.snapshot = {
        "submitted": 2,
        "processed": [{"type": "email", "status": "RETRYING", "value": 1}],
        "latency": [
            {
                "type": "email",
                "count": 2,
                "sum": 3,
                "buckets": {f"{b:g}": int(b >= 1) + int(b >= 2) for b in BUCKETS},
            }
        ],
        "tokens": {"input": 10, "output": 3},
    }
    registry = CollectorRegistry()
    registry.register(collector)
    exposition = generate_latest(registry).decode()
    families = list(text_string_to_metric_families(exposition))
    samples = [sample for family in families for sample in family.samples]
    assert next(s.value for s in samples if s.name == "task_latency_seconds_count") == 2
    assert 'retries_total{type="email"} 1.0' in exposition
    assert 'agent_tokens_total{direction="input"} 10.0' in exposition
    assert all(not {"task_id", "trace_id", "worker_id"} & set(s.labels) for s in samples)


async def test_direct_kafka_helper_propagates_active_trace() -> None:
    """Non-outbox callers can bind or explicitly provide the task's durable trace."""
    from unittest.mock import AsyncMock

    from shared.kafka import publish_json

    producer = AsyncMock()
    identifier, trace = uuid4(), uuid4()
    with log_context(trace_id=trace):
        await publish_json(
            producer, "tasks.pending", key=str(identifier), payload={"task_id": str(identifier)}
        )
    headers = dict(producer.send_and_wait.await_args.kwargs["headers"])
    assert headers["trace_id"] == trace.hex.encode()
    assert headers["task_id"] == str(identifier).encode()
