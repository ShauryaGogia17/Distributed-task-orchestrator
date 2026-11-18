"""FastAPI application with transactional submissions and a recoverable outbox."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from prometheus_client import CONTENT_TYPE_LATEST
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from starlette.exceptions import HTTPException as StarletteHTTPException

from services.api.dlq import router as dlq_router
from services.api.errors import (
    RequestIDMiddleware,
    database_error,
    http_error,
    internal_error,
    validation_error,
)
from services.api.outbox import OutboxPublisher
from services.api.priority import message_key
from services.api.schemas import (
    AttemptOut,
    ErrorOut,
    HealthOut,
    ScheduleSubmit,
    TaskDetail,
    TaskPage,
    TaskSubmit,
)
from shared.config import Settings, get_settings
from shared.db import create_engine, create_session_factory, database_now
from shared.kafka import serialize_json
from shared.metrics import ServiceMetrics
from shared.models import TaskMessage, TaskOut, TaskStatus
from shared.redis import create_redis_client
from shared.scheduling import ScheduleOut, SchedulePatch, next_occurrence
from shared.tables import DelayedTask, Outbox, Schedule, Task, TaskAttempt

logger = logging.getLogger(__name__)


def sessions(request: Request) -> async_sessionmaker[AsyncSession]:
    """Resolve the session factory owned by the active app lifespan."""
    return request.app.state.session_factory


async def submit_task(
    factory: async_sessionmaker[AsyncSession],
    body: TaskSubmit,
    *,
    max_message_bytes: int,
    trace_id: UUID | None = None,
) -> tuple[TaskOut, bool]:
    """Deduplicate in PostgreSQL and commit task plus outbox as one atomic unit."""
    values = body.model_dump()
    values["id"] = uuid4()
    values["trace_id"] = trace_id or uuid4()
    values["status"] = TaskStatus.PENDING
    async with factory.begin() as session:
        now = await database_now(session)
        values["scheduled_at"] = body.scheduled_at or now
        task = await session.scalar(
            insert(Task)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Task.idempotency_key])
            .returning(Task)
        )
        created = task is not None
        if task is None:
            task = await session.scalar(
                select(Task).where(Task.idempotency_key == body.idempotency_key)
            )
            if task is None:
                raise RuntimeError("Idempotent submission conflict did not resolve to a task")
        else:
            message = TaskMessage(
                task_id=task.id,
                type=task.type,
                payload=task.payload,
                priority=task.priority,
                max_retries=task.max_retries,
                scheduled_at=task.scheduled_at,
                created_at=task.created_at,
            )
            if len(serialize_json(message)) > max_message_bytes:
                raise HTTPException(
                    413, "Serialized task message exceeds the configured size limit"
                )
            if task.scheduled_at > now:
                task.status = TaskStatus.SCHEDULED
                session.add(DelayedTask(task_id=task.id, release_at=task.scheduled_at))
                await session.flush()
            else:
                session.add(
                    Outbox(
                        topic="tasks.pending",
                        key=message_key(task.id, task.priority),
                        payload=message.model_dump(mode="json"),
                    )
                )
        result = TaskOut.model_validate(task)
    return result, created


def create_app(
    *,
    engine: AsyncEngine | None = None,
    settings: Settings | None = None,
    start_publisher: bool = True,
) -> FastAPI:
    """Build an isolated app; open clients only at startup and dispose owned clients."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configured = settings or get_settings()
        active_engine = engine if engine is not None else create_engine(configured.postgres)
        app.state.settings = configured
        app.state.session_factory = create_session_factory(active_engine)
        app.state.engine = active_engine
        redis_client = create_redis_client(configured.redis)
        app.state.redis = redis_client
        publisher = OutboxPublisher(app.state.session_factory, configured)
        publisher_task = None
        if start_publisher and configured.api.outbox_enabled:
            publisher_task = asyncio.create_task(publisher.run(), name="outbox-publisher")
        app.state.publisher = publisher
        app.state.publisher_task = publisher_task
        app.state.metrics = ServiceMetrics(
            "api", app.state.session_factory, redis_client, configured
        )
        try:
            yield
        finally:
            if publisher_task is not None:
                await publisher.shutdown(publisher_task)
            await redis_client.aclose()
            if engine is None:
                await active_engine.dispose()

    app = FastAPI(
        title="Distributed Task Scheduler API",
        version="0.7.0",
        lifespan=lifespan,
        description=(
            "Durable task submission and at-least-once Kafka dispatch via a transactional outbox."
        ),
        responses={code: {"model": ErrorOut} for code in (404, 405, 409, 413, 422, 500, 503)},
    )
    app.add_middleware(RequestIDMiddleware)
    app.add_exception_handler(StarletteHTTPException, http_error)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(SQLAlchemyError, database_error)
    app.add_exception_handler(Exception, internal_error)
    app.include_router(dlq_router)

    @app.post(
        "/tasks",
        response_model=TaskOut,
        status_code=201,
        tags=["tasks"],
        responses={200: {"model": TaskOut, "description": "Existing idempotent task"}},
    )
    async def create_task(body: TaskSubmit, request: Request, response: Response) -> TaskOut:
        """Commit a new task/event, or return the original task for a repeated key."""
        result, created = await submit_task(
            sessions(request),
            body,
            max_message_bytes=request.app.state.settings.api.max_task_message_bytes,
            trace_id=UUID(request.state.trace_id),
        )
        assert result.trace_id is not None, "Persisted tasks require a trace"
        response.headers["X-Trace-ID"] = result.trace_id.hex
        logger.info(
            "Task submitted", extra={"task_id": str(result.id), "trace_id": result.trace_id.hex}
        )
        response.status_code = 201 if created else 200
        response.headers["Idempotency-Replayed"] = "false" if created else "true"
        response.headers["Location"] = f"/tasks/{result.id}"
        if created and result.status == TaskStatus.SCHEDULED:
            try:
                await request.app.state.redis.zadd(
                    request.app.state.settings.scheduler.queue_key,
                    {str(result.id): result.scheduled_at.timestamp()},
                )
            except RedisError:
                logger.exception("Delayed task committed; scheduler will restore its Redis index")
        return result

    @app.post("/schedules", response_model=ScheduleOut, status_code=201, tags=["schedules"])
    async def create_schedule(
        body: ScheduleSubmit, request: Request, response: Response
    ) -> ScheduleOut:
        """Persist a cron definition and its next occurrence using the database clock."""
        async with sessions(request).begin() as session:
            now = await database_now(session)
            next_run = next_occurrence(body.cron, body.timezone, now)
            envelope = TaskMessage(
                task_id=uuid4(),
                type=body.type,
                payload=body.payload,
                priority=body.priority,
                max_retries=body.max_retries,
                scheduled_at=next_run,
            )
            if (
                len(serialize_json(envelope))
                > request.app.state.settings.api.max_task_message_bytes
            ):
                raise HTTPException(
                    413, "Serialized task message exceeds the configured size limit"
                )
            schedule = Schedule(
                **body.model_dump(exclude={"missed_run_policy"}),
                missed_run_policy=body.missed_run_policy
                or request.app.state.settings.scheduler.missed_run_policy,
                next_run_at=next_run,
            )
            session.add(schedule)
            await session.flush()
            result = ScheduleOut.model_validate(schedule)
        response.headers["Location"] = f"/schedules/{result.id}"
        return result

    @app.get("/schedules/{schedule_id}", response_model=ScheduleOut, tags=["schedules"])
    async def get_schedule(schedule_id: UUID, request: Request) -> ScheduleOut:
        """Inspect the stored definition, enabled state, and materialization cursor."""
        async with sessions(request)() as session:
            schedule = await session.get(Schedule, schedule_id)
            if schedule is None:
                raise HTTPException(404, "Schedule not found")
            return ScheduleOut.model_validate(schedule)

    @app.patch("/schedules/{schedule_id}", response_model=ScheduleOut, tags=["schedules"])
    async def patch_schedule(
        schedule_id: UUID, body: SchedulePatch, request: Request
    ) -> ScheduleOut:
        """Pause future materialization or resume from the next future cron occurrence."""
        async with sessions(request).begin() as session:
            schedule = await session.scalar(
                select(Schedule).where(Schedule.id == schedule_id).with_for_update()
            )
            if schedule is None:
                raise HTTPException(404, "Schedule not found")
            if body.enabled and not schedule.enabled:
                now = await database_now(session)
                schedule.next_run_at = next_occurrence(schedule.cron, schedule.timezone, now)
            schedule.enabled = body.enabled
            await session.flush()
            result = ScheduleOut.model_validate(schedule)
        return result

    @app.get("/tasks", response_model=TaskPage, tags=["tasks"])
    async def list_tasks(
        request: Request,
        status: TaskStatus | None = None,
        schedule_id: UUID | None = None,
        task_type: Annotated[str | None, Query(alias="type", min_length=1, max_length=128)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> TaskPage:
        """List newest tasks first with optional exact status/type filters."""
        predicates = []
        if status is not None:
            predicates.append(Task.status == status)
        if schedule_id is not None:
            predicates.append(Task.schedule_id == schedule_id)
        if task_type is not None:
            predicates.append(Task.type == task_type)
        async with sessions(request)() as session:
            total = await session.scalar(select(func.count()).select_from(Task).where(*predicates))
            tasks = await session.scalars(
                select(Task)
                .where(*predicates)
                .order_by(Task.created_at.desc(), Task.id.desc())
                .limit(limit)
                .offset(offset)
            )
            return TaskPage(
                items=[TaskOut.model_validate(task) for task in tasks],
                total=total or 0,
                limit=limit,
                offset=offset,
            )

    @app.get("/tasks/{task_id}", response_model=TaskDetail, tags=["tasks"])
    async def get_task(task_id: UUID, request: Request) -> TaskDetail:
        """Read current task state and execution attempts ordered by attempt number."""
        async with sessions(request)() as session:
            task = await session.get(Task, task_id)
            if task is None:
                raise HTTPException(404, "Task not found")
            attempts = await session.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation, TaskAttempt.attempt_no)
            )
            return TaskDetail(
                **TaskOut.model_validate(task).model_dump(),
                attempt_history=[AttemptOut.model_validate(attempt) for attempt in attempts],
            )

    @app.post("/tasks/{task_id}/cancel", response_model=TaskOut, tags=["tasks"])
    async def cancel_task(task_id: UUID, request: Request) -> TaskOut:
        """Atomically cancel only a PENDING or SCHEDULED task; otherwise return 409."""
        async with sessions(request).begin() as session:
            task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
            if task is None:
                raise HTTPException(404, "Task not found")
            if task.status not in {TaskStatus.PENDING, TaskStatus.SCHEDULED}:
                raise HTTPException(409, "Only PENDING or SCHEDULED tasks can be cancelled")
            task.status = TaskStatus.CANCELLED
            await session.flush()
            result = TaskOut.model_validate(task)
        return result

    @app.get(
        "/health",
        response_model=HealthOut,
        tags=["operations"],
        responses={503: {"model": HealthOut, "description": "Readiness checks failed"}},
    )
    async def health(request: Request, response: Response) -> HealthOut:
        """Probe the database schema, Redis, Kafka metadata, and background-loop liveness."""
        configured = request.app.state.settings

        async def database_probe() -> None:
            async with request.app.state.engine.connect() as connection:
                await connection.execute(select(Task.id).limit(0))
                await connection.execute(select(Outbox.id).limit(0))

        async def redis_probe() -> None:
            if not await request.app.state.redis.ping():
                raise RuntimeError("Redis did not return PONG")

        async def kafka_probe() -> None:
            client = AIOKafkaAdminClient(
                bootstrap_servers=configured.kafka.bootstrap_servers,
                request_timeout_ms=configured.kafka.request_timeout_ms,
            )
            try:
                await client.start()
                topics = await client.describe_topics(["tasks.pending"])
                if (
                    not topics
                    or topics[0].get("error_code", 0)
                    or len(topics[0]["partitions"]) != 6
                    or any(partition["leader"] < 0 for partition in topics[0]["partitions"])
                ):
                    raise RuntimeError("Kafka priority partitions are not ready")
            finally:
                await client.close()

        async def probe(
            operation: Callable[[], Awaitable[None]],
        ) -> Literal["ok", "fail", "disabled"]:
            try:
                async with asyncio.timeout(configured.api.health_timeout_seconds):
                    await operation()
                return "ok"
            except Exception:
                return "fail"

        results = await asyncio.gather(
            probe(database_probe), probe(redis_probe), probe(kafka_probe)
        )
        checks = dict(zip(("database", "redis", "kafka"), results, strict=True))
        task = request.app.state.publisher_task
        checks["outbox"] = "disabled" if task is None else "fail" if task.done() else "ok"
        ready = "fail" not in checks.values()
        response.status_code = 200 if ready else 503
        return HealthOut(
            status="ok" if ready else "degraded", checks=checks, checked_at=datetime.now(UTC)
        )

    @app.get(
        "/metrics",
        tags=["operations"],
        response_class=Response,
        responses={200: {"content": {"text/plain": {"schema": {"type": "string"}}}}},
    )
    async def metrics(request: Request) -> Response:
        """Read queue size from PostgreSQL so all API replicas expose the same durable count."""
        return Response(
            await request.app.state.metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    return app


app = create_app()
