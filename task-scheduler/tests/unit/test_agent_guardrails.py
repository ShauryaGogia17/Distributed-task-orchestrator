"""Offline category policy, strict repair/step guards, redaction, and Claude transport tests."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import ValidationError

from services.agent.core import cluster_identity
from services.agent.diagnosis import diagnose
from services.agent.guardrails import GuardedCalls, Redactor, Reservation, contains_redaction
from services.agent.llm import AnthropicClient, Completion, MockLLM
from services.agent.policy import decide
from services.agent.schemas import Category, Diagnosis
from shared.config import AgentSettings, LLMSettings, get_settings


def configuration(**options):
    """Do not contact infrastructure; only read the existing validated settings shape."""
    return get_settings().model_copy(update={"agent": AgentSettings(_env_file=None, **options)})


def diagnosis(category: Category = Category.TRANSIENT, **updates) -> Diagnosis:
    """Construct a valid deterministic model result with optional policy variations."""
    values = {
        "root_cause": "Evidence-backed failure.",
        "category": category,
        "confidence": 0.95,
        "suggested_fix": "Apply the documented correction.",
        "safe_to_auto_retry": True,
        "modified_payload": None,
        "recommended_backoff_s": 30,
    }
    return Diagnosis(**{**values, **updates})


@pytest.mark.parametrize(
    ("category", "action"),
    [
        (Category.TRANSIENT, "REQUEUE"),
        (Category.DEPENDENCY_DOWN, "REQUEUE"),
        (Category.BAD_INPUT, "REQUEUE_MODIFIED"),
        (Category.BUG, "ESCALATE"),
        (Category.RESOURCE_EXHAUSTION, "ESCALATE"),
        (Category.UNKNOWN, "ESCALATE"),
    ],
)
def test_each_category_has_the_expected_policy(category, action) -> None:
    """Only safe high-confidence transient/dependency/input diagnoses may recover."""
    value = diagnosis(
        category, modified_payload={"seconds": 0} if category == Category.BAD_INPUT else None
    )
    decision = decide(
        value,
        configuration(),
        failures=1,
        ai_requeues=0,
        current_payload={"seconds": -1},
        representative_payload={"seconds": -1},
    )
    assert decision.action == action


@pytest.mark.parametrize("updates", [{"confidence": 0.5}, {"safe_to_auto_retry": False}])
def test_low_confidence_or_unsafe_diagnosis_escalates(updates) -> None:
    """Provider optimism cannot bypass either confidence or explicit safety checks."""
    assert (
        decide(
            diagnosis(**updates),
            configuration(),
            failures=1,
            ai_requeues=0,
            current_payload={},
            representative_payload={},
        ).action
        == "ESCALATE"
    )


def test_repeated_cluster_failures_open_circuit() -> None:
    """Distinct observed failures trip a circuit only after the confidence gate."""
    assert (
        decide(
            diagnosis(),
            configuration(),
            failures=3,
            ai_requeues=0,
            current_payload={},
            representative_payload={},
        ).action
        == "OPEN_CIRCUIT_ESCALATE"
    )
    assert (
        decide(
            diagnosis(confidence=0.2),
            configuration(),
            failures=3,
            ai_requeues=0,
            current_payload={},
            representative_payload={},
        ).action
        == "ESCALATE"
    )


def test_retry_limit_and_backoff_cap() -> None:
    """AI recovery cannot create an unbounded failure loop or shorten the last retry delay."""
    assert (
        decide(
            diagnosis(),
            configuration(),
            failures=1,
            ai_requeues=1,
            current_payload={},
            representative_payload={},
        ).action
        == "ESCALATE"
    )
    value = decide(
        diagnosis(),
        configuration(),
        failures=1,
        ai_requeues=0,
        current_payload={},
        representative_payload={},
        previous_delay_s=120,
    )
    assert value.backoff_s == 240
    assert (
        decide(
            diagnosis(),
            configuration(max_backoff_seconds=30),
            failures=1,
            ai_requeues=0,
            current_payload={},
            representative_payload={},
            previous_delay_s=100,
        ).action
        == "ESCALATE"
    )


def test_redaction_and_correction_scope() -> None:
    """Do not replace private or differing payloads using a representative's correction."""
    value = diagnosis(Category.BAD_INPUT, modified_payload={"seconds": 0})
    for payload in ({"seconds": -2}, {"seconds": -1, "password": "[REDACTED]"}):
        assert (
            decide(
                value,
                configuration(),
                failures=1,
                ai_requeues=0,
                current_payload=payload,
                representative_payload={"seconds": -1},
            ).action
            == "ESCALATE"
        )


def test_recursive_and_free_text_redaction() -> None:
    """Known credentials, nested PII, auth headers, connection URLs, and traces are scrubbed."""
    redactor = Redactor(["known-secret-123"])
    raw = {
        "apiKey": "api-value",
        "password": "db-value",
        "to": "person@example.com",
        "nested": [
            {"full_name": "Alice Smith", "phone": "+14155552671", "customer_id": "customer-alpha"}
        ],
        "error": (
            "known-secret-123 Authorization: Bearer abc.def.ghi email=person@example.com "
            "phone=+14155552671 ssn=123-45-6789 "
            "url=https://user:password@localhost/ IP=192.168.1.20"
        ),
        "input_tokens": 123,
        "output_tokens": 456,
    }
    clean = redactor.redact(raw)
    text = json.dumps(clean)
    for private in (
        "api-value",
        "db-value",
        "person@example.com",
        "Alice Smith",
        "+14155552671",
        "customer-alpha",
        "known-secret-123",
        "abc.def.ghi",
        "123-45-6789",
        "user:password",
        "192.168.1.20",
    ):
        assert private not in text
    assert clean["input_tokens"] == 123 and clean["output_tokens"] == 456
    assert contains_redaction(clean)


@pytest.mark.parametrize(
    "updates",
    [
        {"confidence": True},
        {"confidence": 1.01},
        {"safe_to_auto_retry": "yes"},
        {"category": "run_shell"},
        {"recommended_backoff_s": float("nan")},
    ],
)
def test_diagnosis_schema_rejects_invalid_values(updates) -> None:
    """Malformed model output cannot become an executable action."""
    with pytest.raises(ValidationError):
        diagnosis(**updates)


class FakeCollector:
    """Exercise tool/repair protocol without network or database fixtures."""

    encode = staticmethod(json.dumps)

    async def call(self, name, arguments):
        """Only the canned task history tool is permitted in these offline tests."""
        assert name == "get_task_history"
        return {"errors": ["Temporary dependency failure"]}


def calls_for(client, **options):
    """Replace only Redis accounting I/O while retaining the actual egress/step guards."""
    calls = GuardedCalls(
        client, None, configuration(**options), Redactor(["secret-value"]), AsyncMock()
    )
    calls.budget.reserve = AsyncMock(return_value=Reservation("test-hour", "id", 1000000, 1000000))
    calls.budget.settle = AsyncMock()
    return calls


def evidence():
    """A redacted, non-sensitive task context accepted by the offline provider."""
    return {
        "task": {"type": "generate_report", "payload": {"seconds": -1}},
        "errors": [{"error": "ValidationError: seconds must be >= 0"}],
    }


async def test_invalid_json_repairs_once_then_escalates() -> None:
    """Exactly two malformed responses produce one repair and a non-retryable fallback."""
    client = MockLLM(["not json", "{}"])
    result = await diagnose(FakeCollector(), calls_for(client), evidence())
    assert len(client.calls) == 2 and result.diagnosis.confidence == 0
    assert "after one" in result.guard_reason


async def test_invalid_json_can_be_repaired() -> None:
    """One corrected valid response can proceed to the independent action policy."""
    client = MockLLM(["not json", diagnosis()])
    result = await diagnose(FakeCollector(), calls_for(client), evidence())
    assert result.guard_reason is None and result.diagnosis.category == Category.TRANSIENT
    assert len(client.calls) == 2


async def test_invalid_final_tool_uses_valid_repair_protocol() -> None:
    """Every assistant tool-use block is paired with a user tool_result before repair."""
    client = MockLLM(
        [
            Completion(
                [{"type": "tool_use", "id": "bad-final", "name": "submit_diagnosis", "input": {}}]
            ),
            diagnosis(),
        ]
    )
    result = await diagnose(FakeCollector(), calls_for(client), evidence())
    assert result.guard_reason is None
    assert client.calls[1][-1]["content"][0]["tool_use_id"] == "bad-final"


async def test_tool_calling_and_maximum_steps() -> None:
    """Read-only tool results continue the conversation; endless tool requests are bounded."""
    client = MockLLM(request_tools=True)
    result = await diagnose(FakeCollector(), calls_for(client), evidence())
    assert result.guard_reason is None and len(client.calls) == 2
    tool = Completion(
        [{"type": "tool_use", "id": "again", "name": "get_task_history", "input": {}}]
    )
    endless = MockLLM([tool] * 5)
    result = await diagnose(FakeCollector(), calls_for(endless, max_steps=2), evidence())
    assert len(endless.calls) == 2 and "steps exhausted" in result.guard_reason


async def test_kill_switch_blocks_egress(monkeypatch) -> None:
    """A runtime kill switch prevents even MockLLM from seeing a request."""
    monkeypatch.setenv("AGENT_KILL_SWITCH", "true")
    client = MockLLM()
    result = await diagnose(FakeCollector(), calls_for(client), evidence())
    assert not client.calls and "kill switch" in result.guard_reason


async def test_budget_exhaustion_never_calls_model() -> None:
    """Budget fallback is a valid low-confidence escalation without unreserved egress."""
    from services.agent.guardrails import BudgetExceeded

    client = MockLLM()
    calls = calls_for(client)
    calls.budget.reserve.side_effect = BudgetExceeded("Hourly budget exhausted")
    result = await diagnose(FakeCollector(), calls, evidence())
    assert not client.calls and "budget exhausted" in result.guard_reason


async def test_anthropic_adapter_tools_retry_and_usage() -> None:
    """Mock HTTP verifies the real wire contract, transient retry, timeout, and token accounting."""
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        assert request.headers["anthropic-version"] == "2023-06-01"
        if len(requests) == 1:
            return httpx.Response(
                429, headers={"retry-after": "0"}, json={"error": {"type": "rate_limit_error"}}
            )
        return httpx.Response(
            200,
            headers={"request-id": "req-test"},
            json={
                "content": [
                    {
                        "type": "tool_use",
                        "id": "diag",
                        "name": "submit_diagnosis",
                        "input": diagnosis().model_dump(mode="json"),
                    }
                ],
                "usage": {"input_tokens": 101, "output_tokens": 55},
            },
        )

    settings = LLMSettings(_env_file=None, provider="anthropic", api_key="test-key", max_attempts=2)
    client = AnthropicClient(settings, transport=httpx.MockTransport(respond))
    try:
        response = await client.complete(
            [{"role": "system", "content": "diagnose"}, {"role": "user", "content": "{}"}],
            [],
            Diagnosis.model_json_schema(),
        )
        assert response.usage.input_tokens == 101 and response.usage.output_tokens == 55
        assert response.usage.attempts == 2 and response.usage.unknown_attempts == 1
        assert response.request_id == "req-test"
        assert requests[-1]["tool_choice"]["name"] == "submit_diagnosis"
    finally:
        await client.aclose()


async def test_anthropic_timeouts_are_bounded() -> None:
    """An unavailable provider cannot cause an unbounded agent retry loop."""
    from services.agent.llm import LLMError

    requests = []

    def respond(request):
        requests.append(request)
        raise httpx.ReadTimeout("unavailable")

    client = AnthropicClient(
        LLMSettings(
            _env_file=None,
            provider="anthropic",
            api_key="test-key",
            max_attempts=2,
            retry_cap_seconds=0,
        ),
        transport=httpx.MockTransport(respond),
    )
    try:
        with pytest.raises(LLMError):
            await client.complete(
                [{"role": "user", "content": "{}"}], [], Diagnosis.model_json_schema()
            )
        assert len(requests) == 2
    finally:
        await client.aclose()


def test_cluster_identity_is_replica_stable() -> None:
    """Window identity includes type/signature, independent of worker or message identity."""
    from datetime import UTC, datetime

    stamp = datetime(2026, 11, 10, 12, 0, 1, tzinfo=UTC)
    first = cluster_identity("send_email", "a" * 64, stamp, 60)
    assert first == cluster_identity("send_email", "a" * 64, stamp.replace(second=59), 60)
    assert first != cluster_identity("generate_report", "a" * 64, stamp, 60)
    assert first != cluster_identity("send_email", "a" * 64, stamp.replace(minute=1), 60)


def test_structured_redaction_preserves_fractional_statistics() -> None:
    """Long decimal fractions must not become unquoted redaction markers in JSON evidence."""
    redactor = Redactor()
    value = {
        "success_rate": 2 / 3,
        "input_tokens": 12345678901234,
        "payload": {"email": "person@example.com"},
        "error": "phone=415-555-1212",
    }
    encoded = json.dumps(value)
    messages = redactor.messages(
        [
            {"role": "user", "content": encoded},
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "stats", "content": encoded}],
            },
        ]
    )
    audited = redactor.redact({"messages": messages})["messages"]
    for content in (
        messages[0]["content"],
        messages[1]["content"][0]["content"],
        audited[0]["content"],
    ):
        result = json.loads(content)
        assert result["success_rate"] == 2 / 3 and result["input_tokens"] == 12345678901234
        assert "person@example.com" not in content and "415-555-1212" not in content
