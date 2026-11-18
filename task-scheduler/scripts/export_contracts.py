#!/usr/bin/env python3
"""Generate checked-in wire schemas, Python fixtures, identity vectors, and shared Redis Lua."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.worker.retry_policy import event_id
from shared import circuit, redis
from shared.config import PROJECT_ROOT
from shared.contracts import DeadLetterMessage, TaskResultMessage
from shared.fingerprint import failure_signature, normalize_failure
from shared.models import ErrorRecord, RetryMessage, TaskMessage
from shared.priority import message_key, pending_partition

JAVA_ROOT = PROJECT_ROOT / "services" / "worker-java"


def generated_files() -> dict[Path, str]:
    """One source of truth keeps Java protocol constants/resources from silently drifting."""
    files = {
        JAVA_ROOT / "src/main/resources/observability.sql": (
            PROJECT_ROOT / "shared/observability.sql"
        ).read_text()
    }
    for name, model in {
        "task-message": TaskMessage,
        "retry-message": RetryMessage,
        "error-record": ErrorRecord,
        "task-result": TaskResultMessage,
        "dead-letter": DeadLetterMessage,
    }.items():
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            **model.model_json_schema(),
        }
        files[PROJECT_ROOT / "shared" / "contracts" / f"{name}.schema.json"] = (
            json.dumps(schema, indent=2, ensure_ascii=False) + "\n"
        )
    identifier = UUID("77342418-78a5-4bdf-8591-ab0467e5a9f4")
    task = TaskMessage(
        task_id=identifier,
        type="java.data_transform",
        generation=2,
        ai_modified=True,
        payload={
            "operation": "uppercase",
            "values": ["hello", "résumé", "東京"],
            "metadata": {
                "large_integer": 9007199254740993,
                "boolean": True,
                "nothing": None,
                "decimal": 1.25,
            },
        },
        priority=8,
        attempt_no=3,
        max_retries=5,
        scheduled_at=datetime(2026, 11, 10, 12, 30, 0, 123456, UTC),
        created_at=datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    )
    files[JAVA_ROOT / "src/test/resources/contracts/python-task.json"] = (
        task.model_dump_json(indent=2) + "\n"
    )
    fingerprints = [
        {
            "error": (
                "request_id=req-alpha task 023ca111-f987-4acd-ab42-332abcaf1651 "
                "failed at 2026-11-10T10:01:02.123Z after 17 seconds"
            ),
            "trace": (
                'File "/app/worker/handler.py", line 123, in run\n'
                "ConnectionError: peer 192.168.1.20:12345"
            ),
        },
        {
            "error": "ValidationError: résumé 123",
            "trace": "java.worker.Handle.run(Handle.java:456)",
        },
        {"error": "worker_id=worker-alpha 0x1234abcd", "trace": ""},
    ]
    for item in fingerprints:
        item.update(
            normalized=normalize_failure(item["error"]),
            signature=failure_signature(item["error"], item["trace"]),
        )
    vectors = {
        "task_id": str(identifier),
        "event_ids": [
            {
                "attempt": attempt,
                "generation": generation,
                "kind": kind,
                "id": str(event_id(identifier, attempt, kind, generation)),
            }
            for attempt in (1, 3)
            for generation in (0, 2)
            for kind in ("result", "retry", "dlq", "redispatch")
        ],
        "partitions": [
            {
                "priority": priority,
                "key": message_key(identifier, priority),
                "partition": pending_partition(message_key(identifier, priority)),
            }
            for priority in (0, 5, 8)
        ],
        "fingerprints": fingerprints,
    }
    files[JAVA_ROOT / "src/test/resources/contracts/python-vectors.json"] = (
        json.dumps(vectors, indent=2, ensure_ascii=False) + "\n"
    )
    for name, content in {
        "release": redis.RELEASE_IF_OWNER,
        "renew": redis.RENEW_IF_OWNER,
        "circuit_acquire": circuit.ACQUIRE,
        "circuit_renew": circuit.RENEW,
        "circuit_finish": circuit.FINISH,
    }.items():
        files[JAVA_ROOT / f"src/main/resources/lua/{name}.lua"] = content
    return files


def main() -> int:
    """--check verifies exact generated artifacts without mutating files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    mismatches = []
    for path, expected in generated_files().items():
        if args.check:
            if not path.exists() or path.read_text() != expected:
                mismatches.append(str(path.relative_to(PROJECT_ROOT)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected)
    if mismatches:
        print("Contract artifacts differ: " + ", ".join(mismatches))
        return 1
    print("[OK] Shared JSON schemas, Python fixtures, vectors, and Java Lua resources are current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
