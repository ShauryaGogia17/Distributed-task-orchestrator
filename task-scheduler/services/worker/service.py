"""Worker service lifecycle and coordination of polling, liveness, and result relay."""

from __future__ import annotations

import asyncio
import logging
import socket
from uuid import uuid4

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.worker.consumer import ConsumerLoop
from services.worker.executor import TaskExecutor
from services.worker.heartbeat import WorkerHeartbeat
from services.worker.metrics import create_server
from services.worker.registry import HandlerContext, load_builtin_handlers, registered_types
from services.worker.retry_engine import RetryEngine
from shared.config import Settings
from shared.logging import log_context
from shared.metrics import ServiceMetrics
from shared.outbox import OutboxPublisher

logger = logging.getLogger(__name__)


class WorkerService:
    """Own worker-scoped clients and keep heartbeats/relay alive throughout draining."""

    def __init__(
        self, factory: async_sessionmaker[AsyncSession], redis: Redis, settings: Settings
    ) -> None:
        self.factory = factory
        self.redis = redis
        self.settings = settings
        self.worker_id = settings.worker.id or f"{socket.gethostname()}-{uuid4().hex[:8]}"
        self.runtime: ConsumerLoop | None = None
        self.stop_requested = False

    def request_stop(self) -> None:
        """Handle SIGTERM/SIGINT even if consumer startup has not completed yet."""
        self.stop_requested = True
        if self.runtime is not None:
            self.runtime.request_stop()

    async def run(self) -> None:
        """Start consumer, heartbeat and relay, then drain before closing any of them."""
        load_builtin_handlers()
        with log_context(worker_id=self.worker_id):
            async with httpx.AsyncClient(
                follow_redirects=False,
                limits=httpx.Limits(max_connections=self.settings.worker.concurrency),
            ) as http_client:
                context = HandlerContext(
                    self.worker_id,
                    http_client,
                    self.settings.worker.http_timeout_seconds,
                    self.settings.worker.http_max_response_bytes,
                )
                executor = TaskExecutor(self.factory, self.redis, self.settings, context)
                metrics = ServiceMetrics("worker", self.factory, self.redis, self.settings)
                executor.inflight = metrics.inflight
                server = (
                    create_server(
                        metrics,
                        self.settings.worker.metrics_host,
                        self.settings.worker.metrics_port,
                    )
                    if self.settings.worker.metrics_enabled
                    else None
                )
                metrics_task = None
                runtime = ConsumerLoop(executor, self.settings)
                self.runtime = runtime
                if self.stop_requested:
                    runtime.request_stop()
                consumer = runtime.build_consumer()
                runtime.consumer = consumer
                heartbeat = WorkerHeartbeat(self.redis, self.worker_id, self.settings.worker)
                publisher = OutboxPublisher(self.factory, self.settings)
                retry_engine = RetryEngine(self.factory, self.settings, executor)
                heartbeat_task = None
                publisher_task = None
                retry_task = None
                try:
                    if server is not None:
                        metrics_task = asyncio.create_task(server.serve(), name="worker-metrics")
                    await consumer.start()
                    await heartbeat.start()
                    heartbeat_task = asyncio.create_task(heartbeat.run(), name="worker-heartbeat")
                    publisher_task = asyncio.create_task(
                        publisher.run(), name="worker-result-outbox"
                    )
                    retry_task = asyncio.create_task(retry_engine.run(), name="worker-retry-engine")

                    def background_failed(task: asyncio.Task[None]) -> None:
                        if not task.cancelled() and task.exception() is not None:
                            logger.error(
                                "Worker component %s failed; stopping worker",
                                task.get_name(),
                                exc_info=task.exception(),
                            )
                            runtime.request_stop()

                    if metrics_task is not None:
                        metrics_task.add_done_callback(background_failed)
                    heartbeat_task.add_done_callback(background_failed)
                    retry_task.add_done_callback(background_failed)
                    logger.info(
                        "Worker ready handlers=%s concurrency=%d",
                        registered_types(),
                        self.settings.worker.concurrency,
                    )
                    await runtime.poll()
                    if heartbeat_task.done() and not heartbeat_task.cancelled():
                        heartbeat_task.result()
                    if retry_task.done() and not retry_task.cancelled():
                        retry_task.result()
                finally:
                    # Runtime drains first, while the publisher and heartbeat still run.
                    await consumer.stop()
                    if retry_task is not None:
                        await retry_engine.shutdown(retry_task)
                    if publisher_task is not None:
                        await publisher.shutdown(publisher_task)
                    if heartbeat_task is not None:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
                    await heartbeat.close()
                    if metrics_task is not None and server is not None:
                        server.should_exit = True
                        await metrics_task
