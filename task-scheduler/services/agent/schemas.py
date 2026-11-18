"""Strict diagnosis and policy contracts shared by providers and the action engine."""

from enum import StrEnum
from typing import Literal

from pydantic import Field, JsonValue

from shared.models import JSONSchema


class Category(StrEnum):
    """Allowed diagnosis categories; an LLM cannot invent executable actions."""

    TRANSIENT = "transient"
    BAD_INPUT = "bad_input"
    DEPENDENCY_DOWN = "dependency_down"
    BUG = "bug"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    UNKNOWN = "unknown"


class Diagnosis(JSONSchema):
    """The only accepted final LLM output; policy is enforced separately in Python."""

    root_cause: str = Field(min_length=1, max_length=4000)
    category: Category
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False, strict=True)
    suggested_fix: str = Field(min_length=1, max_length=4000)
    safe_to_auto_retry: bool = Field(strict=True)
    modified_payload: dict[str, JsonValue] | None
    recommended_backoff_s: float = Field(ge=0, le=86400, allow_inf_nan=False, strict=True)


class Decision(JSONSchema):
    """A policy decision with an auditable reason and a bounded delay."""

    action: Literal["REQUEUE", "REQUEUE_MODIFIED", "OPEN_CIRCUIT_ESCALATE", "ESCALATE"]
    reasoning: str
    backoff_s: float = 0


def fallback(reason: str) -> Diagnosis:
    """Provider, budget, schema, and step failures always produce a non-retryable diagnosis."""
    return Diagnosis(
        root_cause=reason,
        category=Category.UNKNOWN,
        confidence=0,
        suggested_fix="Inspect the retained task history and resolve manually.",
        safe_to_auto_retry=False,
        modified_payload=None,
        recommended_backoff_s=0,
    )
