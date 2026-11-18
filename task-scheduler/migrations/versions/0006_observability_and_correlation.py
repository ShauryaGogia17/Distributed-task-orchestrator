"""persistent task correlation and observability history

Revision ID: 0006
Revises: 0005
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Apply this schema revision."""
    op.add_column(
        "dead_letters",
        sa.Column(
            "trace_id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False
        ),
    )
    op.add_column(
        "outbox",
        sa.Column(
            "trace_id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False
        ),
    )
    op.add_column(
        "tasks",
        sa.Column(
            "trace_id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False
        ),
    )

    op.execute("""
        UPDATE outbox o SET trace_id=t.trace_id FROM tasks t
        WHERE o.payload->>'task_id'=t.id::text;
        UPDATE dead_letters d SET trace_id=t.trace_id FROM tasks t WHERE d.task_id=t.id;
        UPDATE outbox o SET payload=jsonb_set(o.payload, '{duration_seconds}',
            to_jsonb(GREATEST(0, extract(epoch FROM (a.finished_at-a.started_at)))))
        FROM task_attempts a WHERE o.topic='tasks.results' AND a.finished_at IS NOT NULL
            AND o.payload->>'task_id'=a.task_id::text
            AND COALESCE(o.payload->>'generation','0')=a.generation::text
            AND o.payload->>'attempt_no'=a.attempt_no::text
            AND NOT (o.payload ? 'duration_seconds');
        CREATE FUNCTION inherit_task_trace() RETURNS trigger LANGUAGE plpgsql
        SET search_path FROM CURRENT AS $$
        DECLARE identifier text; canonical uuid;
        BEGIN
            IF TG_TABLE_NAME='outbox' THEN identifier=NEW.payload->>'task_id';
            ELSE identifier=NEW.task_id::text; END IF;
            IF identifier ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
                SELECT trace_id INTO canonical FROM tasks WHERE id=identifier::uuid;
            END IF;
            NEW.trace_id=COALESCE(canonical, NEW.trace_id, gen_random_uuid());
            RETURN NEW;
        END $$;
        CREATE TRIGGER outbox_task_trace BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION inherit_task_trace();
        CREATE TRIGGER dead_letter_task_trace BEFORE INSERT ON dead_letters
            FOR EACH ROW EXECUTE FUNCTION inherit_task_trace();
    """)


def downgrade() -> None:
    """Reverse correlation columns; retain optional duration in already-published result history."""
    op.execute("DROP TRIGGER outbox_task_trace ON outbox")
    op.execute("DROP TRIGGER dead_letter_task_trace ON dead_letters")
    op.execute("DROP FUNCTION inherit_task_trace()")
    op.drop_column("tasks", "trace_id")
    op.drop_column("outbox", "trace_id")
    op.drop_column("dead_letters", "trace_id")
