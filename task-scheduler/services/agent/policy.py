"""Deterministic action policy; model confidence never grants direct execution authority."""

from services.agent.guardrails import contains_redaction, kill_switch_enabled
from services.agent.schemas import Category, Decision, Diagnosis
from shared.config import Settings


def decide(
    diagnosis: Diagnosis,
    settings: Settings,
    *,
    failures: int,
    ai_requeues: int,
    current_payload: dict,
    representative_payload: dict | None,
    previous_delay_s: float = 0,
    guard_reason: str | None = None,
) -> Decision:
    """Require evidence, explicit safety, bounded retry count, and safe correction scope."""
    reason = guard_reason
    if kill_switch_enabled(settings):
        reason = "Agent kill switch is enabled"
    if reason:
        return Decision(action="ESCALATE", reasoning=reason)
    if diagnosis.confidence < settings.agent.confidence_threshold:
        return Decision(action="ESCALATE", reasoning="Confidence is below the configured threshold")
    if failures >= settings.agent.circuit_failure_threshold:
        return Decision(
            action="OPEN_CIRCUIT_ESCALATE",
            reasoning="Repeated failures across distinct tasks exceed the cluster threshold",
        )
    if ai_requeues >= settings.agent.max_auto_requeues:
        return Decision(action="ESCALATE", reasoning="Per-task automatic recovery limit exhausted")
    if not diagnosis.safe_to_auto_retry:
        return Decision(
            action="ESCALATE", reasoning="Diagnosis does not explicitly permit automatic retry"
        )
    delay = max(
        settings.agent.min_backoff_seconds,
        diagnosis.recommended_backoff_s,
        previous_delay_s * 2 if previous_delay_s else 0,
    )
    if delay > settings.agent.max_backoff_seconds:
        return Decision(
            action="ESCALATE", reasoning="A safe longer retry delay exceeds the configured cap"
        )
    if diagnosis.category in {Category.TRANSIENT, Category.DEPENDENCY_DOWN}:
        return Decision(
            action="REQUEUE",
            reasoning="High-confidence retry; retain payload and increase backoff",
            backoff_s=delay,
        )
    if diagnosis.category == Category.BAD_INPUT and diagnosis.modified_payload is not None:
        if (
            current_payload != representative_payload
            or contains_redaction(current_payload)
            or contains_redaction(diagnosis.modified_payload)
        ):
            return Decision(
                action="ESCALATE",
                reasoning="Correction is unsafe for differing, redacted, or truncated payloads",
            )
        if diagnosis.modified_payload == current_payload:
            return Decision(
                action="ESCALATE",
                reasoning="Suggested correction does not change the invalid payload",
            )
        return Decision(
            action="REQUEUE_MODIFIED",
            reasoning="High-confidence input correction matches the representative payload",
            backoff_s=delay,
        )
    return Decision(
        action="ESCALATE",
        reasoning="Category requires human investigation or has no safe correction",
    )
