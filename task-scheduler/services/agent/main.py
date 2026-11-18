"""Agent process lifecycle, readiness, and low-cardinality operational metrics."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from services.agent.core import AgentCore
from shared.config import Settings, get_settings
from shared.db import create_engine, create_session_factory
from shared.metrics import ServiceMetrics
from shared.redis import create_redis_client
from shared.tables import AgentAudit, AgentCluster


def create_app(
    *,
    settings: Settings | None = None,
    engine: AsyncEngine | None = None,
    redis: Redis | None = None,
) -> FastAPI:
    """Own one bounded clustering loop; retain durable claims across process death."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configured = settings or get_settings()
        database = engine if engine is not None else create_engine(configured.postgres)
        client = redis if redis is not None else create_redis_client(configured.redis)
        core = AgentCore(create_session_factory(database), client, configured)
        app.state.metrics = ServiceMetrics("agent", core.factory, client, configured)
        runner = asyncio.create_task(core.run(), name="agent-orchestrator")
        app.state.core, app.state.engine, app.state.settings = core, database, configured
        try:
            yield
        finally:
            core.stop_event.set()
            try:
                await asyncio.wait_for(runner, configured.agent.diagnosis_timeout_seconds + 15)
            finally:
                if redis is None:
                    await client.aclose()
                if engine is None:
                    await database.dispose()

    app = FastAPI(title="AI Agent Orchestrator", version="0.8.0", lifespan=lifespan)

    @app.get("/health")
    async def health() -> JSONResponse:
        """Kill-switch/disabled modes are healthy when their dependency checks pass."""
        try:
            async with asyncio.timeout(app.state.settings.api.health_timeout_seconds):
                await app.state.core.redis.ping()
                async with app.state.engine.connect() as connection:
                    await connection.execute(select(AgentCluster.id).limit(0))
                    await connection.execute(select(AgentAudit.id).limit(0))
            ready = app.state.core.running
        except Exception:
            ready = False
        return JSONResponse(
            {
                "status": "ok" if ready else "degraded",
                "enabled": app.state.settings.agent.enabled,
                "kill_switch": app.state.settings.agent.kill_switch,
                "provider": app.state.settings.llm.provider,
            },
            status_code=200 if ready else 503,
        )

    @app.get("/metrics", response_class=Response)
    async def metrics() -> Response:
        """Expose current mode and durable cluster counts without task/cluster ID labels."""
        registry = CollectorRegistry()
        Gauge("agent_enabled", "Agent processing is enabled", registry=registry).set(
            int(app.state.settings.agent.enabled)
        )
        Gauge(
            "agent_kill_switch", "Automatic actions and LLM calls are disabled", registry=registry
        ).set(int(app.state.settings.agent.kill_switch))
        clusters = Gauge(
            "agent_clusters", "Durable clusters by diagnosis status", ["status"], registry=registry
        )
        async with app.state.core.factory() as session:
            counts = (
                await session.execute(
                    select(AgentCluster.status, func.count()).group_by(AgentCluster.status)
                )
            ).all()
        for status, count in counts:
            clusters.labels(status=status).set(count)
        return Response(
            await app.state.metrics.render() + generate_latest(registry),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return app


app = create_app()
