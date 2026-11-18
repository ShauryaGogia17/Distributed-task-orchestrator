"""Dedicated DLQ process with durable ingestion and readiness checks."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from services.dlq.consumer import DLQConsumer
from shared.config import Settings, get_settings
from shared.db import create_engine, create_session_factory
from shared.metrics import ServiceMetrics
from shared.redis import create_redis_client
from shared.tables import DLQReceipt


def create_app(*, settings: Settings | None = None, engine: AsyncEngine | None = None) -> FastAPI:
    """Own one manual-commit Kafka loop; keep offsets uncommitted on storage failure."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configured = settings or get_settings()
        database = engine if engine is not None else create_engine(configured.postgres)
        consumer = DLQConsumer(create_session_factory(database), configured)
        client = create_redis_client(configured.redis)
        app.state.metrics = ServiceMetrics(
            "dlq", create_session_factory(database), client, configured
        )
        runner = asyncio.create_task(consumer.run(), name="dlq-consumer")
        app.state.consumer = consumer
        app.state.engine = database
        app.state.settings = configured
        try:
            yield
        finally:
            consumer.stop_event.set()
            try:
                await asyncio.wait_for(runner, configured.api.outbox_shutdown_timeout_seconds)
            finally:
                await client.aclose()
                if engine is None:
                    await database.dispose()

    app = FastAPI(title="DLQ Pipeline", version="0.7.0", lifespan=lifespan)

    @app.get("/metrics", response_class=Response)
    async def metrics() -> Response:
        """Export the shared durable snapshot and local process telemetry."""
        return Response(
            await app.state.metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    @app.get("/health")
    async def health() -> JSONResponse:
        """Require both Kafka connectivity and the migrated receipt table."""
        consumer = app.state.consumer
        try:
            async with asyncio.timeout(app.state.settings.api.health_timeout_seconds):
                async with app.state.engine.connect() as connection:
                    await connection.execute(select(DLQReceipt.offset).limit(0))
            ready = consumer.running and consumer.connected
        except Exception:
            ready = False
        return JSONResponse(
            {"status": "ok" if ready else "degraded"}, status_code=200 if ready else 503
        )

    return app


app = create_app()
