"""Real Redis/DB scheduler ordering, fencing, cron policies, and killed-leader failover."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import textwrap
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from services.api.main import create_app as create_api
from services.scheduler.core import SchedulerCore
from services.scheduler.leader import LeadershipLost
from services.scheduler.main import create_app as create_scheduler
from services.worker.executor import TaskExecutor
from services.worker.registry import HandlerContext
from shared.config import PROJECT_ROOT, SchedulerSettings, Settings, get_settings
from shared.db import create_session_factory
from shared.kafka import consumer_context, producer_context
from shared.models import TaskMessage, TaskStatus
from shared.outbox import OutboxPublisher
from shared.redis import create_redis_client
from shared.tables import DelayedTask, Outbox, Schedule, Task

pytestmark = pytest.mark.integration


@dataclass
class Resources:
    """One schema and one Redis key namespace, independent of live Docker schedulers."""

    core: SchedulerCore
    settings: Settings
    factory: async_sessionmaker[AsyncSession]
    engine: AsyncEngine
    schema: str


@pytest.fixture
async def scheduler(database) -> AsyncIterator[Resources]:
    """Avoid the production scheduled_tasks/leader keys while using real services."""
    engine, schema = database
    prefix = f"scheduler-test:{uuid4().hex}"
    configured = get_settings().model_copy(
        update={
            "scheduler": SchedulerSettings(
                _env_file=None,
                queue_key=f"{prefix}:due",
                leader_key=f"{prefix}:leader",
                poll_seconds=0.05,
                election_poll_seconds=0.02,
                leader_ttl_seconds=0.9,
                renewal_seconds=0.15,
                misfire_grace_seconds=0.5,
            )
        }
    )
    redis = create_redis_client(configured.redis)
    factory = create_session_factory(engine)
    core = SchedulerCore(factory, redis, configured)
    try:
        yield Resources(core, configured, factory, engine, schema)
    finally:
        await core.lease.release()
        await redis.delete(configured.scheduler.queue_key, configured.scheduler.leader_key)
        await redis.aclose()


async def database_now(resources):
    """Use the same clock as schedule eligibility and materialization."""
    async with resources.factory() as session:
        return await session.scalar(select(func.clock_timestamp()))


async def delayed(resources, when):
    """Persist a held task in the exact state created by POST /tasks."""
    async with resources.factory.begin() as session:
        task = Task(type="generate_report", payload={"seconds": 0}, scheduled_at=when)
        session.add(task)
        await session.flush()
        task.status = TaskStatus.SCHEDULED
        session.add(DelayedTask(task_id=task.id, release_at=when))
        identifier = task.id
    return identifier


def peer(resources):
    """Create a second actual core competing for the same Redis/DB work."""
    return SchedulerCore(resources.factory, resources.core.redis, resources.settings)


async def test_due_order_future_exclusion_and_one_release(scheduler) -> None:
    """Lua pops ascending due scores and SQL release cannot create duplicate events."""
    now = await database_now(scheduler)
    second = await delayed(scheduler, now - timedelta(seconds=2))
    third = await delayed(scheduler, now - timedelta(seconds=1))
    first = await delayed(scheduler, now - timedelta(seconds=3))
    future = await delayed(scheduler, now + timedelta(seconds=60))
    assert await scheduler.core.lease.acquire()
    assert await scheduler.core.reconcile() == 4
    identifiers = await scheduler.core.pop_due(now)
    assert identifiers == [first, second, third]
    assert (
        await scheduler.core.redis.zscore(scheduler.settings.scheduler.queue_key, str(future))
        is not None
    )
    assert [await scheduler.core.release_one(item) for item in identifiers] == [True] * 3
    assert not await scheduler.core.release_one(first)
    async with scheduler.factory() as session:
        events = list(await session.scalars(select(Outbox).order_by(Outbox.created_at)))
        assert [UUID(row.payload["task_id"]) for row in events] == identifiers
        assert (await session.get(Task, future)).status == TaskStatus.SCHEDULED
        assert (await session.get(Task, first)).status == TaskStatus.PENDING


async def test_two_schedulers_do_not_double_publish(scheduler) -> None:
    """Competing leases, leader transfer, and two relays still publish one event."""
    other = peer(scheduler)
    outcomes = await asyncio.gather(scheduler.core.lease.acquire(), other.lease.acquire())
    assert sum(outcomes) == 1
    owner, standby = (scheduler.core, other) if outcomes[0] else (other, scheduler.core)
    identifier = await delayed(scheduler, await database_now(scheduler) - timedelta(seconds=1))
    assert await owner.tick() == 1
    with pytest.raises(LeadershipLost):
        await standby.tick()
    await owner.lease.release()
    assert await standby.lease.acquire()
    assert await standby.tick() == 0
    async with scheduler.factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
    async with producer_context() as producer:
        relays = [OutboxPublisher(scheduler.factory, scheduler.settings) for _ in range(2)]
        assert sum(await asyncio.gather(*(relay.publish_batch(producer) for relay in relays))) == 1
        assert await relays[0].publish_batch(producer) == 0
    async with consumer_context(
        "tasks.pending", group_id=f"scheduler-publish-{uuid4()}"
    ) as consumer:
        async with asyncio.timeout(10):
            while True:
                record = await consumer.getone()
                if record.value.get("task_id") == str(identifier):
                    break
        assert TaskMessage.model_validate(record.value).task_id == identifier
    await standby.lease.release()


async def test_recovery_after_redis_pop_without_db_commit(scheduler) -> None:
    """A replacement leader restores a popped item from the durable ledger."""
    identifier = await delayed(scheduler, await database_now(scheduler) - timedelta(seconds=1))
    assert await scheduler.core.lease.acquire()
    await scheduler.core.reconcile()
    assert await scheduler.core.pop_due(await database_now(scheduler)) == [identifier]
    assert await scheduler.core.redis.zcard(scheduler.settings.scheduler.queue_key) == 0
    other = peer(scheduler)
    await scheduler.core.redis.pexpire(scheduler.settings.scheduler.leader_key, 1)
    await asyncio.sleep(0.01)
    assert await other.lease.acquire()
    assert await other.tick() == 1
    with pytest.raises(LeadershipLost):
        await scheduler.core.release_one(identifier)
    await scheduler.core.lease.release()
    assert await other.lease.is_owner()
    await other.lease.release()


@pytest.mark.parametrize(("policy", "expected"), [("fire_once", 1), ("skip", 0)])
async def test_missed_cron_runs_are_coalesced_or_skipped(scheduler, policy, expected) -> None:
    """Downtime never causes one task for every missed occurrence."""
    now = await database_now(scheduler)
    async with scheduler.factory.begin() as session:
        schedule = Schedule(
            type="generate_report",
            cron="* * * * *",
            timezone="UTC",
            next_run_at=now.replace(second=0, microsecond=0) - timedelta(minutes=10),
            missed_run_policy=policy,
        )
        session.add(schedule)
        await session.flush()
        identifier = schedule.id
    assert await scheduler.core.lease.acquire()
    # Row locks and occurrence identity also protect racing calls within a leader.
    assert (
        sum(
            await asyncio.gather(
                scheduler.core.materialize_recurring(), scheduler.core.materialize_recurring()
            )
        )
        == expected
    )
    assert await scheduler.core.materialize_recurring() == 0
    async with scheduler.factory() as session:
        tasks = list(await session.scalars(select(Task).where(Task.schedule_id == identifier)))
        assert len(tasks) == expected
        row = await session.get(Schedule, identifier)
        assert row.next_run_at > now
        if expected:
            assert tasks[0].scheduled_for == now.replace(second=0, microsecond=0)
            assert (await session.get(DelayedTask, tasks[0].id)).dispatched_at is None


async def test_future_api_task_is_durable_indexed_and_not_executed_early(scheduler) -> None:
    """Submission retains future work in Redis/DB without prematurely sending it to Kafka."""
    app = create_api(engine=scheduler.engine, settings=scheduler.settings, start_publisher=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            future = await database_now(scheduler) + timedelta(seconds=60)
            body = {
                "type": "generate_report",
                "scheduled_at": future.isoformat(),
                "idempotency_key": "future",
            }
            response = await client.post("/tasks", json=body)
            assert response.status_code == 201
            identifier = UUID(response.json()["id"])
            assert response.json()["status"] == "SCHEDULED"
            assert (await client.post("/tasks", json=body)).json()["id"] == str(identifier)
            score = await scheduler.core.redis.zscore(
                scheduler.settings.scheduler.queue_key, str(identifier)
            )
            assert score == pytest.approx(future.timestamp())
            async with scheduler.factory() as session:
                assert await session.get(DelayedTask, identifier) is not None
                assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
            executor = TaskExecutor(
                scheduler.factory,
                scheduler.core.redis,
                scheduler.settings,
                HandlerContext("early-test", client),
            )
            message = TaskMessage(task_id=identifier, type="generate_report", scheduled_at=future)
            assert await executor.process(message)
            async with scheduler.factory() as session:
                assert (await session.get(Task, identifier)).attempts == 0
            assert (await client.post(f"/tasks/{identifier}/cancel")).status_code == 200
    assert await scheduler.core.lease.acquire()
    await scheduler.core.reconcile()
    assert (
        await scheduler.core.redis.zscore(scheduler.settings.scheduler.queue_key, str(identifier))
        is None
    )


async def test_cron_api_validation_defaults_and_pause(scheduler) -> None:
    """POST stores a valid calendar; pause/resume makes examples operationally manageable."""
    configured = scheduler.settings.model_copy(
        update={
            "scheduler": scheduler.settings.scheduler.model_copy(
                update={"missed_run_policy": "skip"}
            )
        }
    )
    app = create_api(engine=scheduler.engine, settings=configured, start_publisher=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (
                await client.post("/schedules", json={"type": "generate_report", "cron": "bad"})
            ).status_code == 422
            response = await client.post(
                "/schedules",
                json={"type": "generate_report", "cron": "*/5 * * * *", "priority": "high"},
            )
            assert response.status_code == 201, response.text
            definition = response.json()
            assert definition["missed_run_policy"] == "skip"
            assert definition["timezone"] == "UTC" and definition["priority"] == 9
            url = f"/schedules/{definition['id']}"
            assert (await client.get(url)).json()["id"] == definition["id"]
            assert (await client.patch(url, json={"enabled": False})).json()["enabled"] is False
            assert (await client.patch(url, json={"enabled": True})).json()["enabled"] is True
            assert (await client.get(f"/schedules/{uuid4()}")).status_code == 404


async def test_killed_leader_fails_over_and_releases_due_task(scheduler) -> None:
    """SIGKILL a real scheduler process; standby takes over after the unrenewed TTL."""
    env = os.environ.copy()
    env.update(
        {
            "SCHEDULER_ID": "killed-leader",
            "SCHEDULER_QUEUE_KEY": scheduler.settings.scheduler.queue_key,
            "SCHEDULER_LEADER_KEY": scheduler.settings.scheduler.leader_key,
            "SCHEDULER_LEADER_TTL_SECONDS": "0.9",
            "SCHEDULER_RENEWAL_SECONDS": "0.15",
            "SCHEDULER_POLL_SECONDS": "0.05",
            "SCHEDULER_ELECTION_POLL_SECONDS": "0.02",
            "SCHEDULER_TEST_SCHEMA": scheduler.schema,
        }
    )
    code = textwrap.dedent("""
        import asyncio, os
        from services.scheduler.core import SchedulerCore
        from shared.config import get_settings
        from shared.db import create_engine, create_session_factory
        from shared.redis import create_redis_client
        async def main():
            settings = get_settings()
            mapping = {None: os.environ['SCHEDULER_TEST_SCHEMA']}
            engine = create_engine().execution_options(schema_translate_map=mapping)
            client = create_redis_client()
            await SchedulerCore(create_session_factory(engine), client, settings).run()
        asyncio.run(main())
    """)
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        code,
        env=env,
        cwd=PROJECT_ROOT,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    standby = peer(scheduler)
    running = None
    try:
        async with asyncio.timeout(5):
            while True:
                value = await scheduler.core.redis.get(scheduler.settings.scheduler.leader_key)
                if value and json.loads(value)["instance_id"] == "killed-leader":
                    break
                if child.returncode is not None:
                    pytest.fail((await child.stderr.read()).decode())
                await asyncio.sleep(0.02)
        running = asyncio.create_task(standby.run())
        identifier = await delayed(
            scheduler, await database_now(scheduler) + timedelta(seconds=1.1)
        )
        child.kill()
        await child.wait()
        assert child.returncode != 0
        started = asyncio.get_running_loop().time()
        async with asyncio.timeout(4):
            while True:
                if await standby.lease.is_owner():
                    break
                await asyncio.sleep(0.02)
            assert asyncio.get_running_loop().time() - started < 3
            while True:
                async with scheduler.factory() as session:
                    record = await session.get(DelayedTask, identifier)
                if record.dispatched_at is not None:
                    break
                await asyncio.sleep(0.02)
        async with scheduler.factory() as session:
            assert (await session.get(Task, identifier)).status == TaskStatus.PENDING
            assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
        standby.stop_event.set()
        if running is not None:
            await running


async def test_metrics_and_standby_health(scheduler) -> None:
    """Both replicas are healthy while exactly one metric reports authoritative ownership."""
    first = create_scheduler(
        engine=scheduler.engine, redis=scheduler.core.redis, settings=scheduler.settings
    )
    second = create_scheduler(
        engine=scheduler.engine, redis=scheduler.core.redis, settings=scheduler.settings
    )
    async with first.router.lifespan_context(first), second.router.lifespan_context(second):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=first), base_url="http://first"
        ) as one:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=second), base_url="http://second"
            ) as two:
                async with asyncio.timeout(3):
                    while True:
                        snapshots = [
                            (await one.get("/health")).json(),
                            (await two.get("/health")).json(),
                        ]
                        if sum(item["leader"] for item in snapshots) == 1:
                            break
                        await asyncio.sleep(0.02)
                assert all(item["status"] == "ok" for item in snapshots)
                values = []
                for client in (one, two):
                    text = (await client.get("/metrics")).text
                    value = next(
                        line
                        for line in text.splitlines()
                        if line.startswith("scheduler_is_leader{")
                    )
                    values.append(float(value.rsplit(" ", 1)[1]))
                assert sorted(values) == [0, 1]


async def test_leader_renews_past_initial_ttl_and_releases_on_stop(scheduler) -> None:
    """A healthy leader survives its original TTL; graceful stop enables immediate takeover."""
    running = asyncio.create_task(scheduler.core.run())
    other = peer(scheduler)
    try:
        async with asyncio.timeout(2):
            while True:
                if await scheduler.core.lease.is_owner():
                    break
                await asyncio.sleep(0.01)
        await asyncio.sleep(scheduler.settings.scheduler.leader_ttl_seconds + 0.2)
        assert await scheduler.core.lease.is_owner()
        assert not await other.lease.acquire()
    finally:
        scheduler.core.stop_event.set()
        await running
    assert await other.lease.acquire()
    await other.lease.release()


async def test_migration_preserves_legacy_delayed_delivery(scheduler) -> None:
    """Up/down migration retains a dispatch path for pre-scheduler future tasks."""
    identifier, event = uuid4(), uuid4()
    async with scheduler.engine.begin() as connection:
        await connection.execute(text(f'SET LOCAL search_path TO "{scheduler.schema}"'))
        config = Config(str(PROJECT_ROOT / "alembic.ini"))

        def change(sync, revision: str, down: bool = False) -> None:
            config.attributes["connection"] = sync
            (command.downgrade if down else command.upgrade)(config, revision)

        await connection.run_sync(lambda sync: change(sync, "0002", down=True))
        await connection.execute(
            text("""
            INSERT INTO tasks (id, type, scheduled_at)
            VALUES (:id, 'generate_report', clock_timestamp() + interval '1 hour')
        """),
            {"id": identifier},
        )
        await connection.execute(
            text("""
            INSERT INTO outbox (id, topic, key, payload)
            VALUES (:event, 'tasks.pending', :key, jsonb_build_object('task_id', CAST(:id AS text)))
        """),
            {"event": event, "key": f"normal:{identifier}", "id": identifier},
        )
        await connection.run_sync(lambda sync: change(sync, "head"))
        assert (
            await connection.scalar(select(Task.status).where(Task.id == identifier))
            == TaskStatus.SCHEDULED
        )
        assert await connection.scalar(select(DelayedTask.task_id)) == identifier
        assert await connection.scalar(select(func.count()).select_from(Outbox)) == 0
        await connection.run_sync(lambda sync: change(sync, "0002", down=True))
        status = await connection.scalar(
            text("SELECT status FROM tasks WHERE id = :id"), {"id": identifier}
        )
        assert status == "PENDING"
        payload = await connection.scalar(text("SELECT payload FROM outbox"))
        assert TaskMessage.model_validate(payload).task_id == identifier
        await connection.run_sync(lambda sync: change(sync, "head"))
