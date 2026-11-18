"""Concurrency bounds, partition ordering, manual commits, and graceful draining."""

import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from aiokafka.structs import TopicPartition

from services.worker.consumer import ConsumerLoop, RebalanceListener
from shared.config import (
    APISettings,
    KafkaSettings,
    LLMSettings,
    PostgresSettings,
    RedisSettings,
    Settings,
    WorkerSettings,
)


def configuration(**worker_options) -> Settings:
    """Use explicit unit-test settings without reading developer credentials."""
    return Settings(
        _env_file=None,
        postgres=PostgresSettings(_env_file=None, password="test-only"),
        kafka=KafkaSettings(_env_file=None),
        redis=RedisSettings(_env_file=None),
        llm=LLMSettings(_env_file=None, provider="mock", api_key=""),
        api=APISettings(_env_file=None),
        worker=WorkerSettings(_env_file=None, **worker_options),
    )


def record(partition: int, offset: int):
    """Create a valid minimal record envelope without requiring a Kafka broker."""
    return SimpleNamespace(
        topic="tasks.pending",
        partition=partition,
        offset=offset,
        value={
            "task_id": str(uuid4()),
            "type": "test",
            "scheduled_at": "2026-01-01T00:00:00Z",
        },
    )


class FakeConsumer:
    """Deliver only unpaused partitions, with observable commits and explicit seeks."""

    def __init__(self, records):
        self.records = deque(records)
        self.assigned = {TopicPartition(item.topic, item.partition) for item in records}
        self.paused = set()
        self.commit = AsyncMock()
        self.seek_calls = []
        self.sought = asyncio.Event()

    def assignment(self):
        return set(self.assigned)

    def pause(self, *partitions):
        self.paused.update(partitions)

    def resume(self, *partitions):
        self.paused.difference_update(partitions)

    def seek(self, partition, offset):
        self.seek_calls.append((partition, offset))
        self.sought.set()

    async def getmany(self, **kwargs):
        await asyncio.sleep(0.001)
        for item in list(self.records):
            partition = TopicPartition(item.topic, item.partition)
            if partition not in self.paused:
                self.records.remove(item)
                return {partition: [item]}
        return {}


async def test_shutdown_finishes_inflight_and_never_commits_past_unstarted_work() -> None:
    """SIGTERM stops admission, drains two handlers, and commits exactly those offsets."""
    consumer = FakeConsumer([record(0, 0), record(0, 1), record(1, 0), record(2, 0)])
    started = []
    ready = asyncio.Event()
    finish = asyncio.Event()

    async def process(message):
        started.append(message.task_id)
        if len(started) == 2:
            ready.set()
        await finish.wait()
        return True

    executor = SimpleNamespace(process=process, context=SimpleNamespace(worker_id="unit"))
    loop = ConsumerLoop(executor, configuration(concurrency=2), consumer)
    running = asyncio.create_task(loop.poll())
    try:
        async with asyncio.timeout(2):
            await ready.wait()
        assert len(loop.active) == 2
        consumer.commit.assert_not_awaited()
        loop.request_stop()
        await asyncio.sleep(0.01)
        assert not running.done()
        assert len(started) == 2
        finish.set()
        await running
        commits = [call.args[0] for call in consumer.commit.await_args_list]
        assert {next(iter(value.items())) for value in commits} == {
            (TopicPartition("tasks.pending", 0), 1),
            (TopicPartition("tasks.pending", 1), 1),
        }
        assert not loop.active
    finally:
        finish.set()
        loop.request_stop()
        await running


async def test_shutdown_deadline_cancels_without_acknowledgement() -> None:
    """Unfinished attempts remain uncommitted when the grace period expires."""
    consumer = FakeConsumer([record(0, 0)])
    entered = asyncio.Event()

    async def process(message):
        entered.set()
        await asyncio.Event().wait()

    executor = SimpleNamespace(process=process, context=SimpleNamespace(worker_id="unit"))
    loop = ConsumerLoop(executor, configuration(shutdown_timeout_seconds=0.02), consumer)
    running = asyncio.create_task(loop.poll())
    async with asyncio.timeout(2):
        await entered.wait()
        loop.request_stop()
        await running
    consumer.commit.assert_not_awaited()
    assert not loop.active


async def test_rebalance_drains_before_releasing_partition() -> None:
    """Revocation must wait for its record and commit it before ownership transfers."""
    consumer = FakeConsumer([record(0, 0), record(0, 1)])
    entered, finish = asyncio.Event(), asyncio.Event()

    async def process(message):
        entered.set()
        await finish.wait()
        return True

    executor = SimpleNamespace(process=process, context=SimpleNamespace(worker_id="unit"))
    loop = ConsumerLoop(executor, configuration(), consumer)
    running = asyncio.create_task(loop.poll())
    partition = TopicPartition("tasks.pending", 0)
    async with asyncio.timeout(2):
        await entered.wait()
        revoke = asyncio.create_task(RebalanceListener(loop).on_partitions_revoked({partition}))
        await asyncio.sleep(0.01)
        assert not revoke.done()
        finish.set()
        await revoke
        consumer.assigned.clear()
        loop.request_stop()
        await running
    consumer.commit.assert_awaited_once_with({partition: 1})


async def test_processing_failure_seeks_without_committing() -> None:
    """Infrastructure failures must not silently consume their Kafka input."""
    consumer = FakeConsumer([record(0, 7)])
    executor = SimpleNamespace(
        process=AsyncMock(side_effect=ConnectionError("offline")),
        context=SimpleNamespace(worker_id="unit"),
    )
    loop = ConsumerLoop(executor, configuration(), consumer)
    running = asyncio.create_task(loop.poll())
    async with asyncio.timeout(2):
        await consumer.sought.wait()
        loop.request_stop()
        await running
    assert consumer.seek_calls == [(TopicPartition("tasks.pending", 0), 7)]
    consumer.commit.assert_not_awaited()
