"""Bounded per-partition execution with explicit, contiguous Kafka commits."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import ConsumerRecord, TopicPartition

from services.worker.executor import TaskExecutor
from shared.config import Settings
from shared.kafka import commit_record, deserialize_json
from shared.logging import log_context
from shared.models import TaskMessage
from shared.tracing import record_trace

logger = logging.getLogger(__name__)


@dataclass
class InFlight:
    """One owned partition's record and its asynchronous execution."""

    record: ConsumerRecord
    task: asyncio.Task[bool]


class RebalanceListener(ConsumerRebalanceListener):
    """Drain owned work before Kafka transfers partition ownership."""

    def __init__(self, runtime: ConsumerLoop) -> None:
        self.runtime = runtime

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        """Finish and commit completed work, leaving unfinished offsets uncommitted."""
        self.runtime.revoking.update(revoked)
        await self.runtime.drain(revoked, self.runtime.settings.worker.rebalance_timeout_seconds)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        """Permit polling again after the new generation owns its partitions."""
        self.runtime.revoking.difference_update(assigned)
        logger.info("Assigned partitions %s", sorted(partition.partition for partition in assigned))


class ConsumerLoop:
    """Keep one record per partition active and at most WORKER_CONCURRENCY overall.

    Paused partitions still get periodic consumer polls for membership liveness.
    Serial execution per partition prevents later offsets acknowledging unfinished
    earlier records, including during rebalance and SIGTERM draining.
    """

    def __init__(
        self, executor: TaskExecutor, settings: Settings, consumer: AIOKafkaConsumer | None = None
    ) -> None:
        """Allow a test consumer while production creates its client inside run()."""
        self.executor = executor
        self.settings = settings
        self.consumer = consumer
        self.stop_event = asyncio.Event()
        self.active: dict[TopicPartition, InFlight] = {}
        self.revoking: set[TopicPartition] = set()
        self.retry_at: dict[TopicPartition, float] = {}
        self.semaphore = asyncio.Semaphore(settings.worker.concurrency)

    def request_stop(self) -> None:
        """Signal graceful shutdown without cancelling handlers or their leases."""
        self.stop_event.set()

    def build_consumer(self) -> AIOKafkaConsumer:
        """Read raw values so schema failures do not silently advance offsets."""
        kafka, worker = self.settings.kafka, self.settings.worker
        consumer = AIOKafkaConsumer(
            bootstrap_servers=kafka.bootstrap_servers,
            client_id=f"{kafka.client_id}-{self.executor.context.worker_id}",
            group_id=worker.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=kafka.request_timeout_ms,
            max_poll_records=1,
            max_poll_interval_ms=worker.max_poll_interval_ms,
            rebalance_timeout_ms=int(worker.rebalance_timeout_seconds * 1000),
            fetch_max_bytes=8 * 1024 * 1024,
            max_partition_fetch_bytes=1024 * 1024,
        )
        consumer.subscribe(topics=["tasks.pending"], listener=RebalanceListener(self))
        return consumer

    async def execute_record(self, record: ConsumerRecord) -> bool:
        """Validate a dispatch before execution and always release its concurrency slot."""
        try:
            raw = record.value
            if isinstance(raw, bytes | str):
                raw = deserialize_json(raw.encode() if isinstance(raw, str) else raw)
            if (
                isinstance(raw, dict)
                and isinstance(raw.get("type"), str)
                and raw["type"].strip().startswith("java.")
            ):
                return True
            message = TaskMessage.model_validate(raw)
            with log_context(
                task_id=message.task_id, trace_id=record_trace(getattr(record, "headers", None))
            ):
                return await self.executor.process(message)
        finally:
            self.semaphore.release()

    async def finish_job(self, partition: TopicPartition, job: InFlight, *, retry: bool) -> None:
        """Acknowledge only safe completions; seek back after processing/commit failures."""
        assert self.consumer is not None
        if self.active.get(partition) is not job:
            return
        safe = False
        try:
            if not job.task.cancelled():
                safe = job.task.result()
            if safe:
                await commit_record(self.consumer, job.record)
        except Exception:
            safe = False
            logger.exception(
                "Record not acknowledged partition=%d offset=%d",
                partition.partition,
                job.record.offset,
            )
        finally:
            if self.active.get(partition) is job:
                self.active.pop(partition)
        if not safe and retry and partition in self.consumer.assignment():
            self.consumer.seek(partition, job.record.offset)
            self.retry_at[partition] = time.monotonic() + self.settings.worker.retry_poll_seconds

    async def drain(self, partitions: set[TopicPartition], deadline_seconds: float) -> None:
        """Wait for owned jobs, cancel deadline overruns, then commit completed records."""
        jobs = {partition: job for partition, job in self.active.items() if partition in partitions}
        if not jobs:
            return
        _, pending = await asyncio.wait(
            [job.task for job in jobs.values()], timeout=deadline_seconds
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for partition, job in jobs.items():
            await self.finish_job(partition, job, retry=False)

    async def poll(self) -> None:
        """Run until stopped; callers start/stop the consumer around this loop."""
        assert self.consumer is not None
        try:
            while not self.stop_event.is_set():
                for partition, job in list(self.active.items()):
                    if job.task.done() and partition not in self.revoking:
                        await self.finish_job(partition, job, retry=True)
                assigned = self.consumer.assignment()
                blocked = {
                    partition
                    for partition in assigned
                    if partition in self.active
                    or partition in self.revoking
                    or self.retry_at.get(partition, 0) > time.monotonic()
                }
                if len(self.active) >= self.settings.worker.concurrency:
                    blocked = set(assigned)
                if blocked:
                    self.consumer.pause(*blocked)
                ready = assigned - blocked
                if ready:
                    self.consumer.resume(*ready)
                batches = await self.consumer.getmany(timeout_ms=100, max_records=1)
                for partition, records in batches.items():
                    for record in records:
                        if (
                            self.stop_event.is_set()
                            or partition in self.revoking
                            or partition not in self.consumer.assignment()
                        ):
                            continue
                        if partition in self.active:
                            raise RuntimeError("Consumer returned a paused in-flight partition")
                        await self.semaphore.acquire()
                        job = InFlight(record, asyncio.create_task(self.execute_record(record)))
                        self.active[partition] = job
        finally:
            assigned = self.consumer.assignment()
            if assigned:
                self.consumer.pause(*assigned)
            await self.drain(set(self.active), self.settings.worker.shutdown_timeout_seconds)
