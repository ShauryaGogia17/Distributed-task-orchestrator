"""Stable priority lanes for the six-partition tasks.pending topic."""

from hashlib import sha256
from uuid import UUID


def priority_name(priority: int) -> str:
    """Bucket numeric priorities into high (7–9), normal (3–6), and low (0–2)."""
    if not 0 <= priority <= 9:
        raise ValueError("priority must be between 0 and 9")
    return "high" if priority >= 7 else "normal" if priority >= 3 else "low"


def message_key(task_id: UUID, priority: int) -> str:
    """Persist a key that exposes the lane while retaining the unique task identity."""
    return f"{priority_name(priority)}:{task_id}"


def pending_partition(key: str) -> int:
    """Hash the task identity onto one of two fixed partitions for its priority lane."""
    lane, separator, identity = key.partition(":")
    if not separator or lane not in {"high", "normal", "low"}:
        raise ValueError("tasks.pending key must be '<high|normal|low>:<task UUID>'")
    task_id = UUID(identity)
    base = {"high": 0, "normal": 2, "low": 4}[lane]
    return base + sha256(task_id.bytes).digest()[0] % 2
