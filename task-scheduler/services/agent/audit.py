"""Durable redacted agent audit events; escalation uses a reviewable local sink."""

import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from services.agent.guardrails import Redactor
from shared.tables import AgentAudit


class AuditLog:
    """Commit egress and tool events independently of later diagnosis/action failures."""

    def __init__(
        self, factory: async_sessionmaker[AsyncSession], redactor: Redactor, cluster_id: UUID
    ) -> None:
        self.factory, self.redactor, self.cluster_id = factory, redactor, cluster_id
        self.correlation: dict[str, str | None] = {}

    async def record(self, event: str, details: dict[str, Any]) -> None:
        """Append one sanitized event without logging raw secrets or PII to stdout."""
        async with self.factory.begin() as session:
            session.add(
                AgentAudit(
                    cluster_id=self.cluster_id, event=event, details=self.redactor.redact(details)
                )
            )
        logging.getLogger(__name__).info("Agent audit event=%s", event, extra=self.correlation)


def escalation_event(
    session: AsyncSession, cluster_id: UUID | None, incident_id: UUID, reason: str
) -> None:
    """Slack/webhook stub: persist a durable notification, with no external delivery configured."""
    session.add(
        AgentAudit(
            cluster_id=cluster_id,
            dead_letter_id=incident_id,
            event="ESCALATION_NOTIFICATION",
            details={"sink": "local_webhook_stub", "reason": reason},
        )
    )
