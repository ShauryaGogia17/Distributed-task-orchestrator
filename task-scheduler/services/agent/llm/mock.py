"""Deterministic offline diagnoses and scripted tool/repair responses for tests."""

import json
from collections import deque
from typing import Any

from services.agent.llm.base import Completion, LLMClient, Usage
from services.agent.schemas import Category, Diagnosis


class MockLLM(LLMClient):
    """Use explicit responses when supplied; otherwise classify redacted collector evidence."""

    def __init__(
        self,
        responses: list[Completion | Diagnosis | str] | None = None,
        *,
        request_tools: bool = False,
    ) -> None:
        self.responses = deque(responses or [])
        self.calls: list[list[dict[str, Any]]] = []
        self.request_tools = request_tools

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], schema: dict[str, Any]
    ) -> Completion:
        """Count deterministic usage and never issue network requests."""
        self.calls.append(json.loads(json.dumps(messages)))
        if self.responses:
            response = self.responses.popleft()
            if isinstance(response, Completion):
                return response
            text = response.model_dump_json() if isinstance(response, Diagnosis) else response
        elif self.request_tools and not any(
            isinstance(message.get("content"), list) for message in messages
        ):
            return Completion(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mock-history",
                        "name": "get_task_history",
                        "input": {},
                    }
                ],
                usage=Usage(100, 30),
            )
        else:
            evidence = json.loads(
                next(
                    message["content"]
                    for message in messages
                    if message["role"] == "user" and isinstance(message["content"], str)
                )
            )
            errors = json.dumps(evidence.get("errors", [])).lower()
            task = evidence.get("task") or {}
            payload = task.get("payload", {})
            category, modified = Category.UNKNOWN, None
            if "validationerror" in errors:
                category = Category.BAD_INPUT
                if task.get("type") == "generate_report" and (
                    isinstance(payload.get("seconds"), str)
                    or (isinstance(payload.get("seconds"), (int, float)) and payload["seconds"] < 0)
                ):
                    modified = {**payload, "seconds": 0}
            elif any(
                word in errors for word in ("connecterror", "connectionerror", "503", "dependency")
            ):
                category = Category.DEPENDENCY_DOWN
            elif any(word in errors for word in ("timeouterror", "retryableerror", "temporary")):
                category = Category.TRANSIENT
            elif any(word in errors for word in ("memoryerror", "out of memory", "resource")):
                category = Category.RESOURCE_EXHAUSTION
            elif any(word in errors for word in ("typeerror", "nameerror", "assertionerror")):
                category = Category.BUG
            diagnosis = Diagnosis(
                root_cause=f"Offline evidence indicates {category.value}.",
                category=category,
                confidence=0.95 if category != Category.UNKNOWN else 0.4,
                suggested_fix="Correct invalid report delay."
                if modified
                else "Inspect the failing dependency or handler.",
                safe_to_auto_retry=category in {Category.TRANSIENT, Category.DEPENDENCY_DOWN}
                or modified is not None,
                modified_payload=modified,
                recommended_backoff_s=30,
            )
            text = diagnosis.model_dump_json()
        return Completion(
            content=[{"type": "text", "text": text}],
            usage=Usage(max(1, len(json.dumps(messages)) // 4), max(1, len(text) // 4)),
            request_id=f"mock-{len(self.calls)}",
        )
