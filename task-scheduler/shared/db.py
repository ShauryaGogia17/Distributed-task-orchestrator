"""Async database lifecycle, transaction-scoped sessions, and locked transitions."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from functools import lru_cache
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from shared.config import PostgresSettings
from shared.models import InvalidTransition, TaskStatus
from shared.tables import Task


def create_engine(settings: PostgresSettings | None = None) -> AsyncEngine:
    """Build a lazy async psycopg engine; dispose it before its event loop closes."""
    settings = settings or PostgresSettings()
    return create_async_engine(
        settings.sqlalchemy_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": settings.connect_timeout_seconds},
        hide_parameters=True,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create independent sessions with readable attributes after commit."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@lru_cache
def get_engine() -> AsyncEngine:
    """Return one engine per service process; no connection is opened on import."""
    return create_engine()


@lru_cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the process session factory for the current event loop."""
    return create_session_factory(get_engine())


async def close_database() -> None:
    """Dispose the cached engine during service shutdown and clear both caches."""
    if get_engine.cache_info().currsize:
        await get_engine().dispose()
    get_session_factory.cache_clear()
    get_engine.cache_clear()


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession] | None = None,
) -> AsyncIterator[AsyncSession]:
    """Commit on successful exit, otherwise roll back and always close the session."""
    factory = factory or get_session_factory()
    async with factory.begin() as session:
        yield session


async def get_session() -> AsyncIterator[AsyncSession]:
    """Yield a transactional session for a future FastAPI dependency."""
    async with session_scope() as session:
        yield session


async def transition_task(
    session: AsyncSession,
    task_id: UUID,
    target: TaskStatus,
    *,
    expected_status: TaskStatus | None = None,
) -> Task:
    """Lock and refresh a task before validating a change in the caller's transaction.

    Call before editing this task's other fields: refresh replaces any unflushed
    in-memory state. The row lock remains held until the caller commits or rolls back.
    """
    task = await session.scalar(
        select(Task)
        .where(Task.id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if task is None:
        raise LookupError(f"Task {task_id} does not exist")
    if expected_status is not None and task.status != expected_status:
        raise InvalidTransition(f"Expected {expected_status.value}, found {task.status.value}")
    task.status = target
    await session.flush()
    return task


async def database_now(session: AsyncSession) -> datetime:
    """Read a non-null authoritative clock, rejecting invalid driver/session responses."""
    value = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime):
        raise RuntimeError("Database did not return a timestamp")
    return value
