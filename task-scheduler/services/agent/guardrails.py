"""Fail-closed LLM egress, recursive redaction, and atomic shared hourly reservations."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis

from services.agent.llm.base import Completion, LLMClient
from shared.config import Settings
from shared.redis import evaluate

MARKER = "[REDACTED]"
_SENSITIVE = re.compile(
    r"(?i)(?:^|_)(?:password|passwd|secret|api_key|authorization|credentials|access_token|refresh_token|private_key|token|email|recipient|phone|ssn|address|full_name|first_name|last_name|name|dob|user_id|customer_id|account_id)$"
)
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE = re.compile(r"(?<!\w)(?:\+\d{1,3}[ .-]?)?(?:\(\d{3}\)|\d{3})[ .-]\d{3}[ .-]\d{4}(?!\w)")
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CARD = re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)")
_CREDENTIAL = re.compile(
    r"(?i)\b(?:bearer|basic)\s+[a-z0-9_.+/=-]+|\b(?:sk-ant-|sk_live_|AKIA)[a-z0-9_-]+"
)
_PAIR = re.compile(
    r"""(?i)((?:password|passwd|secret|api[_-]?key|authorization|access[_-]?token|refresh[_-]?token|token|email|phone|ssn|full[_-]?name)["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}]+)"""
)
_URL_AUTH = re.compile(r"(?<=://)[^\s/@:]+:[^\s/@]+@")
_IP = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


_E164 = re.compile(r"(?<!\w)\+\d{8,15}(?!\w)")


def kill_switch_enabled(settings: Settings) -> bool:
    """Config requires restart to unset; a true runtime switch always wins."""
    return settings.agent.kill_switch or os.getenv("AGENT_KILL_SWITCH", "").lower() in {
        "1",
        "true",
        "yes",
    }


class GuardrailViolation(RuntimeError):
    """An LLM call or automatic action is forbidden; caller must escalate."""


class BudgetExceeded(GuardrailViolation):
    """The global hourly token or cost budget cannot accommodate this reservation."""


class Redactor:
    """Redact known credentials, common PII fields, and recognizable free-text patterns."""

    def __init__(self, secrets: list[str] | None = None) -> None:
        self.secrets = [value for value in (secrets or []) if value]

    def text(self, value: str) -> str:
        """Scrub stack traces, log lines, URLs, and JSON strings before any provider call."""
        for secret in sorted(self.secrets, key=len, reverse=True):
            value = value.replace(secret, MARKER)
        value = _URL_AUTH.sub(MARKER + "@", value)
        value = _CREDENTIAL.sub(MARKER, value)
        value = _PAIR.sub(lambda match: match.group(1) + '"' + MARKER + '"', value)
        for pattern in (_EMAIL, _SSN, _PHONE, _E164, _CARD, _IP):
            value = pattern.sub(MARKER, value)
        return value

    def redact(self, value: Any) -> Any:
        """Keep JSON structure and safe diagnostic counters while removing sensitive values."""
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                normalized = re.sub(r"([a-z])([A-Z])", r"\1_\2", str(key)).lower().replace("-", "_")
                result[self.text(str(key))] = (
                    MARKER
                    if normalized == "to" or _SENSITIVE.search(normalized)
                    else self.redact(item)
                )
            return result
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        if isinstance(value, str):
            return self.content(value)
        return value

    def content(self, value: str) -> str:
        """Redact structured JSON before serialization so numeric metrics stay valid JSON."""
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return self.text(value)
        if isinstance(parsed, (dict, list)):
            return json.dumps(self.redact(parsed), ensure_ascii=False, allow_nan=False)
        return self.text(value)

    def messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Preserve protocol names/IDs while scrubbing text and tool argument data."""
        cleaned = []
        for message in messages:
            content = message["content"]
            if isinstance(content, str):
                content = self.content(content)
            else:
                blocks = []
                for original in content:
                    block = dict(original)
                    for field in ("text", "content"):
                        if isinstance(block.get(field), str):
                            block[field] = self.content(block[field])
                    if "input" in block:
                        block["input"] = self.redact(block["input"])
                    blocks.append(block)
                content = blocks
            cleaned.append({**message, "content": content})
        return cleaned


def bounded(value: Any, max_bytes: int) -> Any:
    """Bound already-redacted evidence without producing invalid or cut-off JSON."""
    encoded = json.dumps(value, ensure_ascii=False, default=str).encode()
    if len(encoded) <= max_bytes:
        return value
    return {
        "truncated": True,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "preview": encoded[: max(0, max_bytes - 256)].decode("utf-8", errors="ignore"),
    }


def contains_redaction(value: Any) -> bool:
    """Never write an LLM's redacted placeholder back into a real task payload."""
    text = json.dumps(value, default=str)
    return MARKER in text or '"truncated": true' in text


RESERVE = """
local now = redis.call('TIME')
local key = ARGV[1] .. math.floor(tonumber(now[1]) / 3600)
local tokens = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
if tonumber(redis.call('HGET', key, 'tokens') or '0') + tokens > tonumber(ARGV[5])
 or tonumber(redis.call('HGET', key, 'micro_usd') or '0') + cost > tonumber(ARGV[6]) then
 return {0, key}
end
redis.call('HINCRBY', key, 'tokens', tokens)
redis.call('HINCRBY', key, 'micro_usd', cost)
redis.call('HSET', key, ARGV[2], tokens .. ':' .. cost)
redis.call('EXPIRE', key, 7200)
return {1, key}
"""
SETTLE = """
if not redis.call('HGET', KEYS[1], ARGV[1]) then return 0 end
redis.call('HINCRBY', KEYS[1], 'tokens', tonumber(ARGV[2]) - tonumber(ARGV[4]))
redis.call('HINCRBY', KEYS[1], 'micro_usd', tonumber(ARGV[3]) - tonumber(ARGV[5]))
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
"""


@dataclass(frozen=True)
class Reservation:
    """An idempotently settled reservation; crashes conservatively retain the full charge."""

    key: str
    id: str
    tokens: int
    micro_usd: int


class HourlyBudget:
    """Use Redis time and atomic reservations across all agent replicas."""

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings

    def cost(self, input_tokens: int, output_tokens: int) -> int:
        """Convert configurable per-million USD rates into integer micro-dollar accounting."""
        return math.ceil(
            input_tokens * self.settings.llm.input_usd_per_million
            + output_tokens * self.settings.llm.output_usd_per_million
        )

    async def reserve(self, tokens: int, micro_usd: int) -> Reservation:
        """Reserve the worst case before egress; Redis failures also prevent the call."""
        identifier = f"reservation:{uuid4().hex}"
        allowed, key = await evaluate(
            self.redis,
            RESERVE,
            0,
            self.settings.agent.budget_key_prefix,
            identifier,
            tokens,
            micro_usd,
            self.settings.agent.hourly_token_budget,
            math.floor(self.settings.agent.hourly_cost_budget_usd * 1000000),
        )
        if not allowed:
            raise BudgetExceeded("Hourly token/cost budget exhausted")
        return Reservation(key, identifier, tokens, micro_usd)

    async def settle(self, reservation: Reservation, tokens: int, micro_usd: int) -> None:
        """Refund known unused usage once; unknown attempts retain conservative charges."""
        await evaluate(
            self.redis,
            SETTLE,
            1,
            reservation.key,
            reservation.id,
            tokens,
            micro_usd,
            reservation.tokens,
            reservation.micro_usd,
        )


class GuardedCalls:
    """All diagnosis/repair/tool turns pass the same step, redaction, budget, and audit gate."""

    def __init__(
        self,
        client: LLMClient,
        redis: Redis,
        settings: Settings,
        redactor: Redactor,
        audit: Callable[[str, dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.client, self.settings, self.redactor, self.audit = client, settings, redactor, audit
        self.budget = HourlyBudget(redis, settings)
        self.steps = 0
        self.usage = {"input_tokens": 0, "output_tokens": 0, "charged_tokens": 0, "micro_usd": 0}

    def require_enabled(self) -> None:
        """The environment kill switch also takes effect if changed within the running process."""
        if kill_switch_enabled(self.settings):
            raise GuardrailViolation("Agent kill switch is enabled")

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], schema: dict[str, Any]
    ) -> Completion:
        """Reserve all provider attempts, audit before egress, then account for known usage."""
        self.require_enabled()
        if self.steps >= self.settings.agent.max_steps:
            raise GuardrailViolation("Maximum agent steps exhausted")
        sanitized = self.redactor.messages(messages)
        body = json.dumps(
            {"messages": sanitized, "tools": tools, "schema": schema}, ensure_ascii=False
        ).encode()
        if len(body) > self.settings.agent.context_max_bytes:
            raise GuardrailViolation("Redacted LLM context exceeds byte limit")
        input_bound = len(body) + 8192  # UTF-8 bytes plus conservative provider framing slack.
        output_bound = self.settings.llm.max_tokens
        attempts = self.client.max_attempts
        reserve_tokens = (input_bound + output_bound) * attempts
        reserve_cost = self.budget.cost(input_bound, output_bound) * attempts
        reservation = await self.budget.reserve(reserve_tokens, reserve_cost)
        self.steps += 1
        await self.audit(
            "LLM_REQUEST",
            {
                "step": self.steps,
                "prompt_sha256": hashlib.sha256(body).hexdigest(),
                "messages": sanitized,
                "reserved_tokens": reserve_tokens,
                "reserved_micro_usd": reserve_cost,
            },
        )
        try:
            self.require_enabled()
            result = await self.client.complete(sanitized, tools, schema)
        except BaseException:
            self.usage["charged_tokens"] += reserve_tokens
            self.usage["micro_usd"] += reserve_cost
            await self.audit(
                "LLM_FAILED",
                {
                    "step": self.steps,
                    "charged_tokens": reserve_tokens,
                    "charged_micro_usd": reserve_cost,
                    "usage_unknown": True,
                },
            )
            raise
        used = result.usage
        known_tokens = max(0, used.input_tokens) + max(0, used.output_tokens)
        charged_tokens = known_tokens + used.unknown_attempts * (input_bound + output_bound)
        charged_cost = self.budget.cost(
            max(0, used.input_tokens), max(0, used.output_tokens)
        ) + used.unknown_attempts * self.budget.cost(input_bound, output_bound)
        await self.budget.settle(reservation, charged_tokens, charged_cost)
        self.usage["input_tokens"] += used.input_tokens
        self.usage["output_tokens"] += used.output_tokens
        self.usage["charged_tokens"] += charged_tokens
        self.usage["micro_usd"] += charged_cost
        await self.audit(
            "LLM_RESPONSE",
            {
                "step": self.steps,
                "request_id": result.request_id,
                "usage": vars(used),
                "charged_tokens": charged_tokens,
                "micro_usd": charged_cost,
                "content": self.redactor.redact(result.content),
            },
        )
        return result
