"""JSON Kafka clients with explicit ownership, acknowledgements, and manual commits."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import ConsumerRecord, RecordMetadata, TopicPartition
from pydantic import BaseModel

from shared.config import KafkaSettings
from shared.logging import current_trace_id
from shared.tracing import headers_for
from shared.tracing import trace_id as normalize_trace


def serialize_json(value: BaseModel | dict[str, Any]) -> bytes:
    """Encode UTF-8 JSON with native Pydantic UUID/datetime/enum support."""
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
        "utf-8"
    )


def deserialize_json(value: bytes) -> dict[str, Any]:
    """Require an object envelope and reject nonstandard NaN/Infinity constants."""

    def reject_constant(constant: str) -> None:
        raise ValueError(f"Invalid JSON numeric constant: {constant}")

    decoded = json.loads(value.decode("utf-8"), parse_constant=reject_constant)
    if not isinstance(decoded, dict):
        raise ValueError("Kafka JSON payload must be an object")
    return decoded


def create_producer(
    settings: KafkaSettings | None = None,
    *,
    enable_idempotence: bool = True,
) -> AIOKafkaProducer:
    """Build a stopped producer with broker acknowledgements and retry idempotence."""
    settings = settings or KafkaSettings()
    return AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        request_timeout_ms=settings.request_timeout_ms,
        max_request_size=settings.max_message_bytes,
        value_serializer=serialize_json,
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        enable_idempotence=enable_idempotence,
        acks="all",
    )


def create_consumer(
    *topics: str,
    group_id: str,
    settings: KafkaSettings | None = None,
    rebalance_listener: ConsumerRebalanceListener | None = None,
) -> AIOKafkaConsumer:
    """Build a stopped consumer; callers must commit only after durable handling.

    Process each partition in order. On rebalance, finish/commit outstanding work in
    the supplied listener or allow CommitFailedError to propagate for redelivery.
    """
    if not topics or not all(topics) or not group_id:
        raise ValueError("At least one topic and a nonempty group_id are required")
    settings = settings or KafkaSettings()
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        group_id=group_id,
        request_timeout_ms=settings.request_timeout_ms,
        enable_auto_commit=False,
        max_partition_fetch_bytes=settings.max_message_bytes,
        fetch_max_bytes=settings.max_message_bytes,
        auto_offset_reset="earliest",
        value_deserializer=deserialize_json,
        key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
    )
    consumer.subscribe(topics=topics, listener=rebalance_listener)
    return consumer


@asynccontextmanager
async def producer_context(
    settings: KafkaSettings | None = None,
    *,
    enable_idempotence: bool = True,
) -> AsyncIterator[AIOKafkaProducer]:
    """Start a producer and flush/stop it on exit, including startup failure."""
    producer = create_producer(settings, enable_idempotence=enable_idempotence)
    try:
        await producer.start()
        yield producer
    finally:
        await producer.stop()


@asynccontextmanager
async def consumer_context(
    *topics: str,
    group_id: str,
    settings: KafkaSettings | None = None,
    rebalance_listener: ConsumerRebalanceListener | None = None,
) -> AsyncIterator[AIOKafkaConsumer]:
    """Start/stop a consumer without committing unprocessed prefetched messages."""
    consumer = create_consumer(
        *topics, group_id=group_id, settings=settings, rebalance_listener=rebalance_listener
    )
    try:
        await consumer.start()
        yield consumer
    finally:
        await consumer.stop()


async def publish_json(
    producer: AIOKafkaProducer,
    topic: str,
    *,
    key: str,
    payload: BaseModel | dict[str, Any],
    trace_id: str | None = None,
) -> RecordMetadata:
    """Wait for broker acknowledgement before the caller marks an outbox row published."""
    body = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    headers = headers_for(body.get("task_id"), normalize_trace(trace_id or current_trace_id()))
    return await producer.send_and_wait(topic, key=key, value=payload, headers=headers)


async def commit_record(consumer: AIOKafkaConsumer, record: ConsumerRecord) -> None:
    """Commit exactly offset+1 for this partition, never a prefetched batch position."""
    await consumer.commit({TopicPartition(record.topic, record.partition): record.offset + 1})
