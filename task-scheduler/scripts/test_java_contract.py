#!/usr/bin/env python3
"""Run Java 17/Maven contract tests and consume the Java-produced task through Python."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.export_contracts import JAVA_ROOT, generated_files
from services.worker.consumer import ConsumerLoop
from shared.config import JavaWorkerSettings
from shared.models import TaskMessage


async def consume_reverse() -> None:
    """Exercise the actual Python decoder/consumer entry, with no database/network side effects."""
    reverse = (JAVA_ROOT / "target/contracts/java-task.json").read_bytes()
    message = TaskMessage.model_validate_json(reverse)
    assert message.type == "generate_report" and message.payload["origin"] == "java"
    assert message.payload["metadata"]["large_integer"] == 9007199254740993
    assert message.payload["metadata"]["nothing"] is None
    assert message.scheduled_at.microsecond == 123456
    # Only the handler is a spy: the shared Python consumer parses the raw Java bytes.
    executor = SimpleNamespace(process=AsyncMock(return_value=True))
    settings = SimpleNamespace(worker=SimpleNamespace(concurrency=1))
    consumer = ConsumerLoop(executor, settings)
    await consumer.semaphore.acquire()
    assert await consumer.execute_record(SimpleNamespace(value=reverse))
    executor.process.assert_awaited_once_with(message)
    original = TaskMessage.model_validate_json(
        (JAVA_ROOT / "src/test/resources/contracts/python-task.json").read_bytes()
    )
    roundtrip = TaskMessage.model_validate_json(
        (JAVA_ROOT / "target/contracts/python-roundtrip.json").read_bytes()
    )
    assert original == roundtrip
    print(
        "[OK] Java -> Python: consumer decoded task, nested JSON, "
        "large integer, flags, and timestamps"
    )
    print("[OK] Python -> Java -> Python: lossless canonical task roundtrip")


def main() -> int:
    """Use a pinned toolchain, independent of the host's selected Java runtime."""
    for path, expected in generated_files().items():
        if not path.exists() or path.read_text() != expected:
            print("[FAIL] Contract artifacts are stale; run python scripts/export_contracts.py")
            return 1
    configured = JavaWorkerSettings()
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{JAVA_ROOT}:/work",
        "-v",
        "task-scheduler-maven-cache:/root/.m2",
        "-w",
        "/work",
        "-e",
        "MAVEN_OPTS=-Xmx384m",
        configured.maven_image,
        "mvn",
        "-B",
        "-ntp",
        "test",
    ]
    result = subprocess.run(command, check=False)
    if result.returncode:
        return result.returncode
    asyncio.run(consume_reverse())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
