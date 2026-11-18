#!/usr/bin/env python3
"""Verify real Kafka/JPA Java execution, retries, DLQ, and Java-to-Python task production."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx

from shared.config import PROJECT_ROOT, get_settings
from shared.contracts import DeadLetterMessage, TaskResultMessage
from shared.db import create_engine, create_session_factory
from shared.fingerprint import failure_signature
from shared.kafka import consumer_context
from shared.models import TaskMessage
from shared.tables import DeadLetter, Task


async def check() -> None:
    """Submit only uniquely identified demo tasks and verify both real consumer groups."""
    settings = get_settings()
    engine = create_engine(settings.postgres)
    factory = create_session_factory(engine)
    run_id = uuid4().hex
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            java = f"http://127.0.0.1:{settings.java_worker.port}"
            api = f"http://127.0.0.1:{settings.api.port}"
            response = await http.get(java + "/actuator/health")
            response.raise_for_status()
            if response.json()["status"] != "UP":
                raise RuntimeError("Java worker is not healthy")
            async with consumer_context(
                "tasks.results", "tasks.dlq", group_id=f"java-check-{run_id}"
            ) as consumer:
                async with asyncio.timeout(10):
                    while not consumer.assignment():
                        await consumer.getmany(timeout_ms=100, max_records=1)
                await consumer.seek_to_end()
                definitions = {
                    "transform": {
                        "type": "java.data_transform",
                        "payload": {"operation": "scale", "values": [1, 2.5], "factor": 3},
                    },
                    "pdf": {
                        "type": "java.pdf_render",
                        "payload": {"text": "Cross-language PDF", "pages": 2},
                    },
                    "retry": {
                        "type": "java.data_transform",
                        "payload": {
                            "operation": "uppercase",
                            "values": ["retry"],
                            "transient_failures": 1,
                        },
                        "max_retries": 2,
                    },
                    "fatal": {"type": f"java.unsupported_{run_id}", "payload": {}},
                }
                identifiers, tasks, traces = {}, {}, {}
                for name, definition in definitions.items():
                    response = await http.post(
                        api + "/tasks",
                        json={**definition, "idempotency_key": f"java-check-{run_id}-{name}"},
                    )
                    response.raise_for_status()
                    identifiers[name] = response.json()["id"]
                    traces[response.json()["id"]] = UUID(response.json()["trace_id"]).hex
                async with asyncio.timeout(90):
                    while len(tasks) < len(identifiers):
                        for name, identifier in identifiers.items():
                            response = await http.get(api + f"/tasks/{identifier}")
                            response.raise_for_status()
                            task = response.json()
                            terminal = (
                                {"DEAD_LETTERED", "ESCALATED"} if name == "fatal" else {"SUCCEEDED"}
                            )
                            if task["status"] in terminal:
                                tasks[name] = task
                        await asyncio.sleep(0.1)
                for task in tasks.values():
                    if not all(
                        attempt["worker_id"].startswith("java-")
                        for attempt in task["attempt_history"]
                    ):
                        raise RuntimeError("Python executed a Java namespace task")
                    if task["ai_requeues"] != 0:
                        raise RuntimeError("Java fixture unexpectedly required AI recovery")
                if (
                    tasks["transform"]["result"]["values"] != [3, 7.5]
                    or tasks["transform"]["attempts"] != 1
                ):
                    raise RuntimeError("Incorrect Java data transformation")
                pdf = tasks["pdf"]["result"]
                decoded = base64.b64decode(pdf["pdf_base64"], validate=True)
                if (
                    not decoded.startswith(b"%PDF-")
                    or len(decoded) != pdf["size_bytes"]
                    or hashlib.sha256(decoded).hexdigest() != pdf["sha256"]
                    or pdf["pages"] != 2
                ):
                    raise RuntimeError("Java PDF bytes or metadata do not match")
                if tasks["retry"]["attempts"] != 2 or tasks["retry"]["result"]["values"] != [
                    "RETRY"
                ]:
                    raise RuntimeError("Java transient retry did not succeed on attempt two")
                if tasks["fatal"]["attempts"] != 1:
                    raise RuntimeError("Java fatal validation was retried")
                print(
                    "[OK] Python -> Java: shared envelopes, JPA attempts/results, "
                    "real PDF and transformation"
                )
                # A fixture row is committed first; this intentionally exercises a
                # Java producer instead of the API's ordinary transactional outbox.
                async with factory.begin() as session:
                    task = Task(
                        type="generate_report",
                        payload={"seconds": 0, "data": [7, 8], "title": "java-produced"},
                    )
                    session.add(task)
                    await session.flush()
                    produced_id = str(task.id)
                    produced_trace = task.trace_id.hex
                    envelope = TaskMessage(
                        task_id=task.id,
                        type=task.type,
                        payload=task.payload,
                        scheduled_at=task.scheduled_at,
                        created_at=task.created_at,
                    )
                command = [
                    "docker",
                    "compose",
                    "--profile",
                    "java",
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "-e",
                    f"TASK_TRACE_ID={produced_trace}",
                    "--entrypoint",
                    "java",
                    "worker-java",
                    "-cp",
                    "/app/classes:/app/lib/*",
                    "io.taskscheduler.worker.ContractProbe",
                    "emit",
                ]
                completed = await asyncio.to_thread(
                    subprocess.run,
                    command,
                    cwd=PROJECT_ROOT,
                    input=envelope.model_dump_json(),
                    text=True,
                    capture_output=True,
                    timeout=60,
                    check=True,
                )
                lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
                java_task = TaskMessage.model_validate_json(lines[-1])
                if java_task != envelope:
                    raise RuntimeError("Java-produced task changed the contract")
                async with asyncio.timeout(30):
                    while True:
                        response = await http.get(api + f"/tasks/{produced_id}")
                        response.raise_for_status()
                        task = response.json()
                        if task["status"] == "SUCCEEDED":
                            break
                        await asyncio.sleep(0.1)
                if task["result"]["sum"] != 15 or task["attempt_history"][0][
                    "worker_id"
                ].startswith("java-"):
                    raise RuntimeError("Java-produced Python task was not handled correctly")
                print(
                    "[OK] Java -> Python: Java Kafka producer task consumed and SUCCEEDED on Python"
                )
                results, letter = {}, None
                expected = set(identifiers.values())
                async with asyncio.timeout(20):
                    while (
                        set(results) != expected
                        or letter is None
                        or not {"RETRYING", "SUCCEEDED"}.issubset(
                            results.get(identifiers["retry"], set())
                        )
                    ):
                        record = await consumer.getone()
                        if record.value.get("task_id") not in expected:
                            continue
                        headers = dict(record.headers)
                        identifier = record.value["task_id"]
                        if (
                            headers.get("task_id") != identifier.encode()
                            or headers.get("trace_id") != traces[identifier].encode()
                        ):
                            raise RuntimeError("Java task/result/DLQ correlation mismatch")
                        if record.topic == "tasks.results":
                            result = TaskResultMessage.model_validate(record.value)
                            if result.duration_seconds is None:
                                raise RuntimeError("Java execution duration missing")
                            results.setdefault(str(result.task_id), set()).add(result.status)
                        else:
                            letter = DeadLetterMessage.model_validate(record.value)
                if letter.reason != "fatal error" or len(letter.error_history) != 1:
                    raise RuntimeError("Java DLQ message history does not match Python contract")
                async with factory() as session:
                    row = await session.get(DeadLetter, letter.id)
                    last = letter.error_history[-1]
                    if row is None or row.failure_signature != failure_signature(
                        last.error, last.stack_trace
                    ):
                        raise RuntimeError("Java/Python failure signatures differ")
                print(
                    "[OK] Retry/backoff and fatal DLQ use Python-valid JSON "
                    "and identical fingerprints"
                )
                response = await http.get(java + "/actuator/prometheus")
                response.raise_for_status()
                if (
                    "java_worker_attempts_total" not in response.text
                    or "jvm_memory_used_bytes" not in response.text
                ):
                    raise RuntimeError("Java Micrometer metrics missing")
                print("[OK] Actuator health and Micrometer Prometheus metrics")
    finally:
        await engine.dispose()


def main() -> int:
    """Report failure classes without printing credentials or connection environments."""
    try:
        asyncio.run(check())
        return 0
    except Exception as exc:
        print(
            f"[FAIL] Java worker verification: {type(exc).__name__}; "
            "inspect worker-java logs and task history"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
