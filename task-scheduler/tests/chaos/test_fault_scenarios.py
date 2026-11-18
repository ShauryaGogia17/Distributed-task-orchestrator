"""Real fault injection, opt-in and scoped to the labelled Compose test project."""

from collections.abc import AsyncIterator

import pytest

from scripts.chaos import CASES, ChaosSettings, ChaosSuite

pytestmark = pytest.mark.chaos


@pytest.fixture
async def suite() -> AsyncIterator[ChaosSuite]:
    """Own all clients; each fault restores its victim before fixture cleanup."""
    active = ChaosSuite(ChaosSettings())
    try:
        yield active
    finally:
        await active.close()


@pytest.mark.parametrize("case", CASES)
async def test_fault_invariant(suite: ChaosSuite, case: str) -> None:
    """Every scenario emits its own explicit PASS/FAIL and machine-readable evidence."""
    result = await suite.run_case(case)
    assert result["result"] == "PASS"
