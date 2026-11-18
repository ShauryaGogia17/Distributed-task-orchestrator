"""Recoverable Redis due queue and duplicate-safe recurring task materialization."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import socket
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4, uuid5

from prometheus_client import CollectorRegistry, Counter, Gauge
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.scheduler.leader import LeaderLease, LeadershipLost
from shared.config import Settings
from shared.db import database_now
from shared.models import TaskMessage, TaskStatus
from shared.priority import message_key
from shared.redis import evaluate
from shared.scheduling import latest_occurrence, next_occurrence, occurrence_task_id
from shared.tables import DelayedTask, Outbox, Schedule, Task

logger = logging.getLogger(__name__)

POP_DUE = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then return {} end
local items = redis.call('zrangebyscore', KEYS[2], '-inf', ARGV[2],
                        'withscores', 'limit', 0, ARGV[3])
for i = 1, #items, 2 do redis.call('zrem', KEYS[2], items[i]) end
return items
"""

INDEX_IF_LEADER = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
return 1
"""


class SchedulerCore:
    """Only a lease owner ticks; Postgres uniqueness remains the durable safety net."""

    def __init__(
        self, factory: async_sessionmaker[AsyncSession], redis: Redis, settings: Settings
    ) -> None:
        self.factory = factory
        self.redis = redis
        self.settings = settings
        self.instance_id = settings.scheduler.id or f"{socket.gethostname()}-{uuid4().hex[:8]}"
        self.lease = LeaderLease(redis, self.instance_id, settings.scheduler)
        self.stop_event = asyncio.Event()
        self.running = False
        self.registry = CollectorRegistry()
        self.leader_metric = Gauge(
            "scheduler_is_leader",
            "This instance currently owns the leader lease",
            ["instance_id"],
            registry=self.registry,
        ).labels(self.instance_id)
        self.released_metric = Counter(
            "scheduler_released_tasks_total", "Due tasks durably released", registry=self.registry
        )
        self.recurring_metric = Counter(
            "scheduler_materialized_runs_total", "Cron runs materialized", registry=self.registry
        )
        self.skipped_metric = Counter(
            "scheduler_skipped_misfires_total",
            "Coalesced missed runs skipped",
            registry=self.registry,
        )

    async def materialize_recurring(self) -> int:
        """Coalesce or skip downtime, using row locks and a unique occurrence identity."""
        await self.lease.require()
        count = 0
        skipped = 0
        async with self.factory.begin() as session:
            now = await database_now(session)
            schedules = list(
                await session.scalars(
                    select(Schedule)
                    .where(Schedule.enabled.is_(True), Schedule.next_run_at <= now)
                    .order_by(Schedule.next_run_at, Schedule.id)
                    .limit(self.settings.scheduler.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for schedule in schedules:
                occurrence = schedule.next_run_at
                missed = occurrence < now - timedelta(
                    seconds=self.settings.scheduler.misfire_grace_seconds
                )
                if missed and schedule.missed_run_policy == "skip":
                    skipped += 1
                else:
                    if missed:
                        occurrence = latest_occurrence(schedule.cron, schedule.timezone, now)
                    identifier = occurrence_task_id(schedule.id, occurrence)
                    task = await session.scalar(
                        insert(Task)
                        .values(
                            id=identifier,
                            type=schedule.type,
                            payload=schedule.payload,
                            status=TaskStatus.PENDING,
                            priority=schedule.priority,
                            max_retries=schedule.max_retries,
                            scheduled_at=occurrence,
                            schedule_id=schedule.id,
                            scheduled_for=occurrence,
                        )
                        .on_conflict_do_nothing(
                            index_elements=[Task.schedule_id, Task.scheduled_for]
                        )
                        .returning(Task)
                    )
                    if task is not None:
                        task.status = TaskStatus.SCHEDULED
                        session.add(DelayedTask(task_id=task.id, release_at=occurrence))
                        count += 1
                    schedule.last_run_at = occurrence
                schedule.next_run_at = next_occurrence(schedule.cron, schedule.timezone, now)
            await self.lease.require()
        self.recurring_metric.inc(count)
        self.skipped_metric.inc(skipped)
        return count

    async def reconcile(self) -> int:
        """Restore undispatched DB records after a Redis loss or a crash following pop."""
        await self.lease.require()
        count = 0
        async with self.factory.begin() as session:
            rows = list(
                await session.scalars(
                    select(DelayedTask)
                    .where(DelayedTask.dispatched_at.is_(None))
                    .order_by(DelayedTask.release_at, DelayedTask.task_id)
                    .limit(self.settings.scheduler.batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for row in rows:
                task = await session.get(Task, row.task_id)
                if task is None or task.status != TaskStatus.SCHEDULED:
                    row.dispatched_at = await database_now(session)
                    await self.redis.zrem(self.settings.scheduler.queue_key, str(row.task_id))
                    continue
                added = await evaluate(
                    self.redis,
                    INDEX_IF_LEADER,
                    2,
                    self.settings.scheduler.leader_key,
                    self.settings.scheduler.queue_key,
                    self.lease.value,
                    row.release_at.timestamp(),
                    str(row.task_id),
                )
                if not added:
                    raise LeadershipLost("Leadership expired while rebuilding the due index")
                count += 1
            await self.lease.require()
        return count

    async def pop_due(self, now: datetime) -> list[UUID]:
        """Atomically fence, select by ascending score, and remove at most one batch."""
        values = await evaluate(
            self.redis,
            POP_DUE,
            2,
            self.settings.scheduler.leader_key,
            self.settings.scheduler.queue_key,
            self.lease.value,
            now.timestamp(),
            self.settings.scheduler.batch_size,
        )
        return [UUID(values[index]) for index in range(0, len(values), 2)]

    async def release_one(self, task_id: UUID) -> bool:
        """Commit PENDING plus one outbox event; ledger identity prevents double release."""
        await self.lease.require()
        async with self.factory.begin() as session:
            row = await session.scalar(
                select(DelayedTask).where(DelayedTask.task_id == task_id).with_for_update()
            )
            if row is None or row.dispatched_at is not None:
                return False
            task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
            now = await database_now(session)
            if row.release_at > now:
                return False
            if task is None or task.status != TaskStatus.SCHEDULED:
                row.dispatched_at = now
                return False
            task.status = TaskStatus.PENDING
            message = TaskMessage(
                task_id=task.id,
                generation=task.generation,
                ai_modified=task.ai_modified,
                type=task.type,
                payload=task.payload,
                priority=task.priority,
                attempt_no=task.attempts + 1,
                max_retries=task.max_retries,
                scheduled_at=task.scheduled_at,
                created_at=task.created_at,
            )
            session.add(
                Outbox(
                    id=uuid5(task.id, "scheduled-release:v1"),
                    topic="tasks.pending",
                    key=message_key(task.id, task.priority),
                    payload=message.model_dump(mode="json"),
                )
            )
            row.dispatched_at = now
            await self.lease.require()
        logger.info(
            "Scheduled task released",
            extra={"task_id": str(task.id), "trace_id": task.trace_id.hex},
        )
        self.released_metric.inc()
        return True

    async def tick(self) -> int:
        """Materialize, rebuild, and pop due items; DB checks recover every crash window."""
        await self.lease.require()
        await self.materialize_recurring()
        await self.reconcile()
        async with self.factory() as session:
            now = await database_now(session)
        count = 0
        for identifier in await self.pop_due(now):
            if await self.release_one(identifier):
                count += 1
        return count

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), seconds)
        except TimeoutError:
            pass

    async def _renew(self) -> None:
        while self.lease.held and not self.stop_event.is_set():
            await self._pause(self.settings.scheduler.renewal_seconds)
            if not self.stop_event.is_set() and not await self.lease.renew():
                self.leader_metric.set(0)
                return

    async def _heartbeat(self) -> None:
        await self.redis.set(
            f"scheduler:instance:{self.instance_id}",
            json.dumps(
                {
                    "instance_id": self.instance_id,
                    "hostname": socket.gethostname(),
                    "leader": await self.lease.is_owner(),
                    "last_seen": datetime.now(UTC).isoformat(),
                }
            ),
            ex=max(10, math.ceil(self.settings.scheduler.leader_ttl_seconds * 2)),
        )

    async def run(self) -> None:
        """Elect/renew a leader; standby retries promptly and remains independently healthy."""
        renewal = None
        self.running = True
        try:
            while not self.stop_event.is_set():
                try:
                    if renewal is not None and renewal.done():
                        if not renewal.cancelled() and renewal.exception() is not None:
                            renewal.result()
                        renewal = None
                    if not await self.lease.is_owner():
                        if renewal is not None:
                            renewal.cancel()
                            await asyncio.gather(renewal, return_exceptions=True)
                            renewal = None
                        if await self.lease.acquire():
                            renewal = asyncio.create_task(
                                self._renew(), name="scheduler-leader-renewal"
                            )
                            logger.info("Scheduler %s acquired leadership", self.instance_id)
                    self.leader_metric.set(1 if self.lease.held else 0)
                    await self._heartbeat()
                    if self.lease.held:
                        await self.tick()
                        await self._pause(self.settings.scheduler.poll_seconds)
                    else:
                        await self._pause(self.settings.scheduler.election_poll_seconds)
                except Exception:
                    logger.exception("Scheduler tick/election failed; durable work will be retried")
                    self.lease.held = False
                    self.leader_metric.set(0)
                    if renewal is not None:
                        renewal.cancel()
                        await asyncio.gather(renewal, return_exceptions=True)
                        renewal = None
                    await self._pause(self.settings.scheduler.election_poll_seconds)
        finally:
            self.running = False
            if renewal is not None:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
            self.leader_metric.set(0)
            try:
                await self.lease.release()
                await self.redis.delete(f"scheduler:instance:{self.instance_id}")
            except Exception:
                logger.exception("Scheduler exit cleanup failed; Redis TTL will expire leadership")
