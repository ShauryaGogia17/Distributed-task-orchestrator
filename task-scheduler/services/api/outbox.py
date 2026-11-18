"""Compatibility export for the publisher shared by API and worker replicas."""

from shared.outbox import OutboxPublisher

__all__ = ["OutboxPublisher"]
