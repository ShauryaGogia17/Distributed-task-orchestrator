"""Embedded metrics HTTP server that leaves process signal ownership with the worker CLI."""

from collections.abc import Iterator
from contextlib import contextmanager

import uvicorn
from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST

from shared.metrics import ServiceMetrics


class MetricsServer(uvicorn.Server):
    """Run beside the Kafka loop without replacing its SIGTERM drain handler."""

    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        """The worker entry point handles shutdown for all background components."""
        yield


def create_server(metrics: ServiceMetrics, host: str, port: int) -> MetricsServer:
    """Expose telemetry on the private Compose network."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/metrics")
    async def scrape() -> Response:
        """Fail the scrape on dependency errors; never emit a misleading zero snapshot."""
        return Response(await metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    return MetricsServer(
        uvicorn.Config(app, host=host, port=port, log_config=None, access_log=False)
    )
