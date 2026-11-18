"""Context isolation and single-record structured exception output."""

import asyncio
import json
import logging
from io import StringIO

from shared.logging import JSONFormatter, log_context


def make_logger() -> tuple[logging.Logger, StringIO]:
    """Use an isolated logger without changing the pytest process root logger."""
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JSONFormatter())
    logger = logging.Logger("test-json", level=logging.INFO)
    logger.addHandler(handler)
    return logger, stream


def test_nested_context_and_exception_restoration() -> None:
    """Nested worker context retains task ID and restores both after an exception."""
    logger, stream = make_logger()
    with log_context(task_id="task-1", worker_id="worker-1"):
        try:
            with log_context(worker_id="worker-2"):
                raise ValueError("two\nlines")
        except ValueError:
            logger.exception("failure")
        logger.info("restored")
    logger.info("outside")
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert len(rows) == 3
    assert rows[0]["task_id"] == "task-1"
    assert rows[0]["worker_id"] == "worker-1"
    assert "ValueError" in rows[0]["exception"]
    assert rows[1]["worker_id"] == "worker-1"
    assert rows[2]["task_id"] is None
    assert rows[2]["worker_id"] is None


async def test_async_tasks_do_not_leak_context() -> None:
    """Interleaved asyncio tasks must each log their own task and worker IDs."""
    logger, stream = make_logger()
    ready = asyncio.Event()

    async def emit(identifier: str) -> None:
        with log_context(task_id=identifier, worker_id=f"worker-{identifier}"):
            await ready.wait()
            await asyncio.sleep(0)
            with log_context(worker_id="nested"):
                logger.info("nested")
            logger.info("outer")

    first = asyncio.create_task(emit("a"))
    second = asyncio.create_task(emit("b"))
    ready.set()
    await asyncio.gather(first, second)
    rows = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert {(r["task_id"], r["worker_id"]) for r in rows} == {
        ("a", "nested"),
        ("a", "worker-a"),
        ("b", "nested"),
        ("b", "worker-b"),
    }
