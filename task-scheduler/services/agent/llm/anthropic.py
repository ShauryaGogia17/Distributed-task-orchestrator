"""Claude Messages API adapter with bounded retries and owned async HTTP transport."""

import asyncio
import random
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from services.agent.llm.base import Completion, LLMClient, LLMError, Usage
from shared.config import LLMSettings


class AnthropicClient(LLMClient):
    """Use tool schemas and a forced final diagnosis tool for structured JSON output."""

    def __init__(
        self, settings: LLMSettings, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.settings = settings
        self.max_attempts = settings.max_attempts
        self.http = httpx.AsyncClient(
            base_url="https://api.anthropic.com",
            timeout=settings.timeout_seconds,
            transport=transport,
            follow_redirects=False,
            headers={
                "x-api-key": settings.api_key.get_secret_value(),
                "anthropic-version": "2023-06-01",
            },
        )

    def _delay(self, response: httpx.Response | None, attempt: int) -> float:
        """Honor bounded Retry-After seconds/date, otherwise use exponential full jitter."""
        value = response.headers.get("retry-after") if response is not None else None
        if value:
            try:
                delay = float(value)
            except ValueError:
                try:
                    delay = (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds()
                except (ValueError, TypeError):
                    delay = 0
            return max(0, min(delay, self.settings.retry_cap_seconds))
        return random.uniform(0, min(self.settings.retry_cap_seconds, 2 ** (attempt - 1)))

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], schema: dict[str, Any]
    ) -> Completion:
        """Bound every network attempt; the outer guard reserves for all attempts up front."""
        system = "\n".join(
            str(message["content"]) for message in messages if message["role"] == "system"
        )
        diagnosis_tool = {
            "name": "submit_diagnosis",
            "description": "Return the final validated diagnosis.",
            "input_schema": schema,
        }
        body = {
            "model": self.settings.model,
            "max_tokens": self.settings.max_tokens,
            "system": system,
            "messages": [message for message in messages if message["role"] != "system"],
            "tools": [*tools, diagnosis_tool],
            "tool_choice": {"type": "auto"}
            if tools
            else {"type": "tool", "name": "submit_diagnosis"},
        }
        for attempt in range(1, self.max_attempts + 1):
            response = None
            try:
                response = await self.http.post("/v1/messages", json=body)
                response.raise_for_status()
                data = response.json()
                content = data["content"]
                if not isinstance(content, list):
                    raise ValueError("Invalid provider content")
                usage = data["usage"]
                return Completion(
                    content=content,
                    usage=Usage(
                        input_tokens=int(usage["input_tokens"])
                        + int(usage.get("cache_creation_input_tokens", 0))
                        + int(usage.get("cache_read_input_tokens", 0)),
                        output_tokens=int(usage["output_tokens"]),
                        attempts=attempt,
                        unknown_attempts=attempt - 1,
                    ),
                    request_id=response.headers.get("request-id"),
                )
            except httpx.HTTPStatusError as exc:
                if (
                    exc.response.status_code not in {408, 409, 429}
                    and exc.response.status_code < 500
                ):
                    raise LLMError(f"Anthropic HTTP {exc.response.status_code}") from None
            except (httpx.TransportError, ValueError, KeyError, TypeError):
                pass
            if attempt < self.max_attempts:
                await asyncio.sleep(self._delay(response, attempt))
        raise LLMError("Anthropic attempts exhausted; usage may be unknown")

    async def aclose(self) -> None:
        """Close the provider transport after the cluster pipeline finishes."""
        await self.http.aclose()
