"""Compatibility exports for priority routing now shared by all publishers."""

from shared.priority import message_key, pending_partition, priority_name

__all__ = ["message_key", "pending_partition", "priority_name"]
