"""Schema-isolated PostgreSQL fixture shared by service integration tests."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.config import PROJECT_ROOT
from shared.db import create_engine


@pytest.fixture
async def database() -> AsyncIterator[tuple[AsyncEngine, str]]:
    """Migrate a unique schema and clean up only the schema owned by this test."""
    engine = create_engine()
    schema = f"scheduler_test_{uuid4().hex}"
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            config = Config(str(PROJECT_ROOT / "alembic.ini"))

            def upgrade(sync_connection: Connection) -> None:
                config.attributes["connection"] = sync_connection
                command.upgrade(config, "head")

            await connection.run_sync(upgrade)
        yield engine.execution_options(schema_translate_map={None: schema}), schema
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
