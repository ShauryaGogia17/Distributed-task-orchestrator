"""Scheduler lifecycle, standby readiness, and per-instance Prometheus metrics."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from services.scheduler.core import SchedulerCore
from shared.config import Settings, get_settings
from shared.db import create_engine, create_session_factory
from shared.metrics import ServiceMetrics
from shared.outbox import OutboxPublisher
from shared.redis import create_redis_client
from shared.tables import DelayedTask, Schedule


def create_app(
    *,
    settings: Settings | None = None,
    engine: AsyncEngine | None = None,
    redis: Redis | None = None,
) -> FastAPI:
    """Create an isolated scheduler process with owned leader and publication loops."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configured = settings or get_settings()
        database = engine if engine is not None else create_engine(configured.postgres)
        client = redis if redis is not None else create_redis_client(configured.redis)
        factory = create_session_factory(database)
        core = SchedulerCore(factory, client, configured)
        publisher = OutboxPublisher(factory, configured)
        runner = asyncio.create_task(core.run(), name="scheduler")
        relay = asyncio.create_task(publisher.run(), name="scheduler-outbox")
        app.state.metrics = ServiceMetrics(
            "scheduler",
            factory,
            client,
            configured,
            registry=core.registry,
            leader=core.leader_metric,
        )
        app.state.core = core
        app.state.engine = database
        app.state.redis = client
        app.state.settings = configured
        try:
            yield
        finally:
            core.stop_event.set()
            try:
                await asyncio.wait_for(runner, configured.api.outbox_shutdown_timeout_seconds)
            finally:
                await publisher.shutdown(relay)
                if redis is None:
                    await client.aclose()
                if engine is None:
                    await database.dispose()

    app = FastAPI(title="Task Scheduler", version="0.6.0", lifespan=lifespan)

    @app.get("/metrics", response_class=Response)
    async def metrics() -> Response:
        """Report current Redis ownership, not a stale locally cached leader flag."""
        core = app.state.core
        try:
            owned = await core.lease.is_owner()
        except Exception:
            owned = False
        core.leader_metric.set(int(owned))
        return Response(
            await app.state.metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    @app.get("/health")
    async def health() -> JSONResponse:
        """Standbys are healthy when their election loop and dependencies are live."""
        core = app.state.core
        try:
            async with asyncio.timeout(app.state.settings.api.health_timeout_seconds):
                await app.state.redis.ping()
                async with app.state.engine.connect() as connection:
                    await connection.execute(select(DelayedTask.task_id).limit(0))
                    await connection.execute(select(Schedule.id).limit(0))
                leader = await core.lease.is_owner()
            ready = core.running
        except Exception:
            ready, leader = False, False
        return JSONResponse(
            {
                "status": "ok" if ready else "degraded",
                "leader": leader,
                "instance_id": core.instance_id,
            },
            status_code=200 if ready else 503,
        )

    return app


app = create_app()
