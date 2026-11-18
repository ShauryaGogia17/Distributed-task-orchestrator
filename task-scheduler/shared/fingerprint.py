"""Versioned stable error fingerprints for clustering without per-event identifiers."""

import hashlib
import re

_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}[T ][0-9:.]+(?:Z|[+-]\d{2}:?\d{2})?\b")
_UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
_HEX = re.compile(r"\b(?:0x[0-9a-f]+|[0-9a-f]{16,})\b", re.I)
_IDENTIFIER = re.compile(
    r"""(?i)["']?\b((?:(?:task|worker|request|trace|span|user|job|correlation)_?)?id)["']?\s*[:=]\s*["']?[a-z0-9_.:\-]+["']?"""
)
_FILE = re.compile(r'File "([^"]+)"')
_NUMBERS = re.compile(r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def normalize_failure(value: str) -> str:
    """Remove volatile IDs, timestamps, numbers, and absolute traceback path prefixes."""
    normalized = _IDENTIFIER.sub(lambda match: f"{match.group(1).lower()}=<id>", value)
    normalized = _TIMESTAMP.sub("<timestamp>", normalized)
    normalized = _UUID.sub("<id>", normalized)
    normalized = _HEX.sub("<id>", normalized)
    normalized = _FILE.sub(
        lambda match: 'File "<path>/' + re.split(r"[/\\]", match.group(1))[-1] + '"', normalized
    )
    normalized = _NUMBERS.sub("<number>", normalized)
    return " ".join(normalized.lower().split())


def failure_signature(error: str, stack_trace: str | None = None) -> str:
    """SHA-256 of normalized v1 root error and traceback; never use it as event identity."""
    canonical = f"v1\n{normalize_failure(error)}\n{normalize_failure(stack_trace or '')}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def failure_category(reason: str) -> str:
    """Use stable operational categories independent of task type or volatile details."""
    lowered = reason.lower()
    if "poison" in lowered or "crash" in lowered:
        return "poison"
    if "fatal" in lowered or "validation" in lowered:
        return "fatal"
    if "exhaust" in lowered or "budget" in lowered:
        return "exhausted"
    return "unknown"
