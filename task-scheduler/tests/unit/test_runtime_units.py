"""Lifecycle, dependency failures, metrics, and publication boundaries without infrastructure."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fakes import NOW, Factory, Session, task
from prometheus_client import CollectorRegistry, Gauge
from sqlalchemy.ext.asyncio import AsyncEngine

from services.scheduler.leader import LeaderLease, LeadershipLost
from services.worker.heartbeat import WorkerHeartbeat
from shared.config import SchedulerSettings, WorkerSettings, get_settings
from shared.models import TaskStatus
from shared.tables import Outbox


@pytest.mark.parametrize("name", ["api", "agent", "scheduler", "dlq"])
def test_http_entrypoints_read_settings_and_preserve_json_logging(monkeypatch, name):
    module = importlib.import_module(f"services.{name}.__main__")
    run = MagicMock()
    monkeypatch.setattr(module.uvicorn, "run", run)
    monkeypatch.setattr(module, "configure_logging", MagicMock())
    module.main()
    assert run.call_args.kwargs["log_config"] is None
    assert run.call_args.args == (f"services.{name}.main:app",)


async def test_worker_entrypoint_cleans_up_signals_and_clients(monkeypatch):
    from services.worker import __main__ as entry

    engine = MagicMock()
    engine.dispose = AsyncMock()
    redis = AsyncMock()
    service = MagicMock(run=AsyncMock())
    monkeypatch.setattr(entry, "create_engine", lambda _: engine)
    monkeypatch.setattr(entry, "create_redis_client", lambda _: redis)
    monkeypatch.setattr(entry, "WorkerService", lambda *args: service)
    loop = asyncio.get_running_loop()
    add = MagicMock()
    remove = MagicMock()
    monkeypatch.setattr(loop, "add_signal_handler", add)
    monkeypatch.setattr(loop, "remove_signal_handler", remove)
    await entry.serve()
    assert add.call_count == remove.call_count == 2
    redis.aclose.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_worker_heartbeat_owner_loss_and_local_identity(tmp_path):
    settings = WorkerSettings(
        _env_file=None, health_file=tmp_path / "heartbeat", heartbeat_interval_seconds=0.001
    )
    redis = AsyncMock()
    redis.set.return_value = True
    redis.eval.return_value = 0
    heartbeat = WorkerHeartbeat(redis, "unit", settings)
    await heartbeat.start()
    assert json.loads(settings.health_file.read_text())["worker_id"] == "unit"
    with pytest.raises(RuntimeError, match="lost"):
        await heartbeat.run()
    await heartbeat.close()
    assert not settings.health_file.exists()
    redis.set.return_value = False
    with pytest.raises(RuntimeError, match="already active"):
        await heartbeat.start()


@pytest.mark.parametrize("payload", [None, "bad json", "{}", "matching", "different"])
async def test_worker_readiness_requires_matching_live_heartbeat(monkeypatch, tmp_path, payload):
    from services.worker import health

    settings = WorkerSettings(_env_file=None, health_file=tmp_path / "identity")
    redis = AsyncMock()
    redis.get.return_value = "different" if payload == "different" else '{"worker_id":"unit"}'
    monkeypatch.setattr(health, "WorkerSettings", lambda: settings)
    monkeypatch.setattr(health, "create_redis_client", lambda: redis)
    if payload is not None:
        settings.health_file.write_text(
            '{"worker_id":"unit"}' if payload in ("matching", "different") else payload
        )
    assert await health.check() == (payload == "matching")
    redis.aclose.assert_awaited_once()


async def test_leader_lease_never_releases_a_successor():
    redis = AsyncMock()
    redis.set.return_value = True
    redis.eval.return_value = 1
    lease = LeaderLease(redis, "unit", SchedulerSettings(_env_file=None))
    assert await lease.acquire()
    redis.get.return_value = lease.value
    assert await lease.acquire() and await lease.is_owner()
    assert await lease.renew()
    redis.get.return_value = "successor"
    with pytest.raises(LeadershipLost):
        await lease.require()
    await lease.release()
    assert not lease.held
    redis.set.return_value = False
    assert not await lease.acquire()


async def test_metrics_snapshot_is_bounded_and_preserves_labels():
    from shared.metrics import ServiceMetrics

    session = Session(
        execute=[
            None,
            None,
            {
                "submitted": 4,
                "types": ["report"],
                "processed": [],
                "latency": [],
                "dlq": [],
                "diagnoses": [],
                "tokens": {},
            },
        ]
    )
    engine = MagicMock(spec=AsyncEngine)
    engine.get_execution_options.return_value = {"schema_translate_map": {None: "private"}}
    engine.dialect.identifier_preparer.quote_schema.return_value = '"private"'
    session.bind = engine
    redis = AsyncMock()
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[1])
    redis.pipeline = MagicMock(return_value=pipeline)
    metrics = ServiceMetrics("unit", Factory(session), redis, get_settings())
    text = (await metrics.render()).decode()
    assert "tasks_submitted_total 4.0" in text and 'circuit_breaker_open{type="report"} 1.0' in text
    session.bind = None
    with pytest.raises(TypeError):
        await metrics.render()


async def test_outbox_broker_failure_rolls_back_and_replay_keeps_identity():
    from shared.outbox import OutboxPublisher

    row = Outbox(
        id=uuid4(),
        trace_id=uuid4(),
        topic="tasks.pending",
        key=f"normal:{uuid4()}",
        payload={"task_id": str(uuid4())},
        created_at=NOW,
    )
    # Use the public key builder so the unit test does not mirror partition arithmetic.
    from shared.priority import message_key

    row.key = message_key(uuid4(), 5)
    session = Session(scalars=[[row], [row]])
    publisher = OutboxPublisher(Factory(session), get_settings())
    producer = AsyncMock()
    producer.send_and_wait.side_effect = [ConnectionError("broker down"), None]
    with pytest.raises(ConnectionError):
        await publisher.publish_batch(producer)
    assert not session.committed and row.published_at is None
    assert await publisher.publish_batch(producer) == 1
    assert (
        session.committed
        and producer.send_and_wait.call_args_list[0] == producer.send_and_wait.call_args_list[1]
    )
    publisher.stop_event.set()
    await publisher._pause(0.01)


async def test_outbox_delivered_dlq_is_fenced_by_generation():
    from shared.outbox import OutboxPublisher

    row = task(TaskStatus.FAILED, attempts=1)
    event = Outbox(
        id=uuid4(),
        trace_id=row.trace_id,
        topic="tasks.dlq",
        key=str(row.id),
        payload={
            "kind": "task.dead_letter",
            "task_id": str(row.id),
            "attempt_no": 1,
            "generation": 0,
        },
    )
    session = Session(scalar=[row], scalars=[[event]])
    assert await OutboxPublisher(Factory(session), get_settings()).publish_batch(AsyncMock()) == 1
    assert row.status == TaskStatus.DEAD_LETTERED


async def test_database_sessions_clock_and_transition_contract(monkeypatch):
    from shared import db

    session = Session(scalar=[task(), None, task()])
    factory = Factory(session)
    async with db.session_scope(factory) as current:
        assert current is session
    row = await db.transition_task(
        session, uuid4(), TaskStatus.SCHEDULED, expected_status=TaskStatus.PENDING
    )
    assert row.status == TaskStatus.SCHEDULED
    with pytest.raises(LookupError):
        await db.transition_task(session, uuid4(), TaskStatus.SCHEDULED)
    from shared.models import InvalidTransition

    with pytest.raises(InvalidTransition):
        await db.transition_task(
            session, uuid4(), TaskStatus.SCHEDULED, expected_status=TaskStatus.RUNNING
        )
    assert await db.database_now(session) == NOW
    session.scalar = AsyncMock(return_value=None)
    with pytest.raises(RuntimeError):
        await db.database_now(session)
    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(db, "create_engine", lambda: engine)
    db.get_engine.cache_clear()
    db.get_session_factory.cache_clear()
    assert db.get_engine() is engine
    assert db.get_session_factory() is db.get_session_factory()
    await db.close_database()
    engine.dispose.assert_awaited_once()
    monkeypatch.setattr(db, "get_session_factory", lambda: factory)
    dependency = db.get_session()
    assert await anext(dependency) is session
    await dependency.aclose()


@pytest.mark.parametrize("role", ["scheduler", "agent", "dlq"])
async def test_service_lifespans_health_and_metrics(monkeypatch, role):
    module = importlib.import_module(f"services.{role}.main")
    redis = AsyncMock()
    engine = MagicMock()
    engine.dispose = AsyncMock()
    engine.connect.return_value = Session()
    session = Session(execute=[[("DONE", 1)]])
    factory = Factory(session)
    core = SimpleNamespace(
        factory=factory,
        redis=redis,
        stop_event=asyncio.Event(),
        running=True,
        connected=True,
        lease=SimpleNamespace(is_owner=AsyncMock(return_value=True)),
        instance_id="unit",
        registry=CollectorRegistry(),
    )
    core.leader_metric = Gauge("scheduler_is_leader", "leader", registry=core.registry)

    async def run():
        await core.stop_event.wait()

    core.run = run

    class Relay:
        async def run(self):
            await core.stop_event.wait()

        async def shutdown(self, runner):
            await runner

    monkeypatch.setattr(module, "create_engine", lambda _: engine)
    monkeypatch.setattr(module, "create_session_factory", lambda _: factory)
    monkeypatch.setattr(module, "create_redis_client", lambda _: redis)
    monkeypatch.setattr(module.ServiceMetrics, "render", AsyncMock(return_value=b"# metrics\n"))
    monkeypatch.setattr(
        module,
        {"scheduler": "SchedulerCore", "agent": "AgentCore", "dlq": "DLQConsumer"}[role],
        lambda *args: core,
    )
    if role == "scheduler":
        monkeypatch.setattr(module, "OutboxPublisher", lambda *args: Relay())
    app = module.create_app()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://unit") as client,
    ):
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/metrics")).status_code == 200
        engine.connect.return_value.execute.side_effect = ConnectionError("db down")
        assert (await client.get("/health")).status_code == 503
    redis.aclose.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_worker_metrics_http_server_leaves_signals_to_cli(monkeypatch):
    from services.worker.metrics import create_server

    metrics = SimpleNamespace(render=AsyncMock(return_value=b"worker_inflight 1\n"))
    server = create_server(metrics, "127.0.0.1", 0)
    with server.capture_signals():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(server.config.app), base_url="http://unit"
        ) as http:
            response = await http.get("/metrics")
            assert response.text == "worker_inflight 1\n"


@pytest.mark.parametrize("failed", [False, True])
async def test_worker_service_drains_and_cleans_owned_components(monkeypatch, failed):
    from services.worker import service as module

    settings = get_settings().model_copy(
        update={"worker": get_settings().worker.model_copy(update={"metrics_enabled": False})}
    )
    components = []

    class Component:
        def __init__(self, *args):
            self.stop = asyncio.Event()
            self.start = AsyncMock()
            self.close = AsyncMock()
            components.append(self)

        async def run(self):
            if failed and self is components[0]:
                raise ConnectionError("heartbeat down")
            await self.stop.wait()

        async def shutdown(self, runner):
            self.stop.set()
            await runner

    consumer = AsyncMock()
    runtime = MagicMock()
    runtime.build_consumer.return_value = consumer

    async def poll():
        await asyncio.sleep(0.01)

    runtime.poll = AsyncMock(side_effect=poll)
    monkeypatch.setattr(module, "ConsumerLoop", lambda *args: runtime)
    for name in ("WorkerHeartbeat", "OutboxPublisher", "RetryEngine"):
        monkeypatch.setattr(module, name, Component)
    instance = module.WorkerService(Factory(Session()), AsyncMock(), settings)
    instance.request_stop()
    if failed:
        with pytest.raises(ConnectionError):
            await instance.run()
    else:
        await instance.run()
    consumer.stop.assert_awaited_once()
    assert runtime.request_stop.called and all(
        component.close.await_count <= 1 for component in components
    )


@pytest.mark.parametrize("failure", ["connect", "publish", "empty"])
async def test_outbox_loop_reconnects_and_can_be_stopped(monkeypatch, failure):
    from shared import outbox

    relay = outbox.OutboxPublisher(Factory(Session()), get_settings())

    @asynccontextmanager
    async def producer(*args, **kwargs):
        if failure == "connect":
            raise ConnectionError("down")
        yield AsyncMock()

    monkeypatch.setattr(outbox, "producer_context", producer)
    relay.publish_batch = AsyncMock(
        side_effect=ConnectionError("send") if failure == "publish" else None, return_value=0
    )

    async def stop(_):
        relay.stop_event.set()

    relay._pause = stop
    await relay.run()

    async def hanging():
        await asyncio.sleep(10)

    relay.settings = relay.settings.model_copy(
        update={
            "api": relay.settings.api.model_copy(update={"outbox_shutdown_timeout_seconds": 0.001})
        }
    )
    await relay.shutdown(asyncio.create_task(hanging()))


async def test_kafka_contexts_close_clients_even_when_start_fails(monkeypatch):
    from shared import kafka

    producer = AsyncMock()
    consumer = AsyncMock()
    monkeypatch.setattr(kafka, "AIOKafkaProducer", lambda **kwargs: producer)
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda **kwargs: consumer)
    consumer.subscribe = MagicMock()
    async with kafka.producer_context() as active:
        assert active is producer
    async with kafka.consumer_context("tasks.pending", group_id="unit") as active:
        assert active is consumer
    producer.start.side_effect = RuntimeError("metadata")
    with pytest.raises(RuntimeError):
        async with kafka.producer_context():
            pass
    assert producer.stop.await_count == 2 and consumer.stop.await_count == 1
    with pytest.raises(ValueError):
        kafka.create_consumer(group_id="missing-topic")
