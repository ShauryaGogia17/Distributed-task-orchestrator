"""Opt-in Java 17 contract and real heterogeneous worker checks."""

import subprocess
import sys

import pytest

from scripts.check_java_worker import check
from shared.config import PROJECT_ROOT

pytestmark = [pytest.mark.integration, pytest.mark.java]


def test_python_java_contract_roundtrip() -> None:
    """Maven consumes Python fixtures and Python's real consumer consumes Java's output."""
    subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts/test_java_contract.py")],
        cwd=PROJECT_ROOT,
        check=True,
        timeout=180,
    )


async def test_live_java_and_python_workers() -> None:
    """Verify shared Kafka/JPA results, delayed retry, fatal DLQ, and Java-produced Python work."""
    await check()
