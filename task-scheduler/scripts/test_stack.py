#!/usr/bin/env python3
"""Build, run, collect evidence, and remove a dedicated Docker Compose test project."""

from __future__ import annotations

import argparse
import base64
import json
import secrets
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import PROJECT_ROOT


def run(
    arguments: list[str], *, check: bool = True, output: Path | None = None
) -> subprocess.CompletedProcess:
    """Execute argv in the repository; generated credentials are never printed."""
    if output is None:
        return subprocess.run(arguments, cwd=PROJECT_ROOT, check=check)
    with output.open("w") as stream:
        return subprocess.run(
            arguments, cwd=PROJECT_ROOT, check=check, stdout=stream, stderr=subprocess.STDOUT
        )


def main() -> int:
    """Keep failures nonzero and always recover only the project created by this invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["integration", "chaos", "all"], default="all", nargs="?")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--keep", action="store_true", help="Keep only this test project for debugging"
    )
    parser.add_argument(
        "--case",
        choices=[
            "all",
            "worker-crash",
            "kafka-outage",
            "scheduler-failover",
            "agent-timeout",
            "agent-invalid",
            "redis-restart",
        ],
        default="all",
    )
    args = parser.parse_args()
    artifacts = PROJECT_ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    project = "task-scheduler-test-" + uuid4().hex[:10]
    env_file = artifacts / f"{project}.env"
    env_file.write_text(
        "TEST_PROJECT_NAME="
        + project
        + "\nTEST_POSTGRES_PASSWORD="
        + secrets.token_urlsafe(32)
        + "\nTEST_KAFKA_CLUSTER_ID="
        + base64.urlsafe_b64encode(uuid4().bytes).decode().rstrip("=")
        + "\n"
    )
    env_file.chmod(0o600)
    # The command uses its immutable per-run path; this pointer is only for CI/debug cleanup.
    pointer = artifacts / "test-stack.env"
    pointer.write_text(env_file.read_text())
    pointer.chmod(0o600)
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        "docker-compose.test.yml",
        "--profile",
        "test",
        "--profile",
        "chaos",
    ]
    (artifacts / "test-stack.json").write_text(
        json.dumps({"project": project, "mode": args.mode}, indent=2) + "\n"
    )
    print(f"Test project: {project}; no host ports or development volumes are shared.", flush=True)
    result = 1
    try:
        if not args.no_build:
            run([*compose, "build", "runner"])
        run([*compose, "up", "-d", "--wait", "--wait-timeout", "180", "kafka", "redis", "postgres"])
        run([*compose, "up", "-d", "kafka-init", "migrate"])
        run([*compose, "wait", "kafka-init", "migrate"])
        if args.mode in ("integration", "all"):
            run(
                [
                    *compose,
                    "run",
                    "--rm",
                    "--no-deps",
                    "-T",
                    "runner",
                    "python",
                    "-m",
                    "pytest",
                    "tests/integration",
                    "--run-integration",
                    "-q",
                    "--junitxml=artifacts/junit/integration.xml",
                ]
            )
        if args.mode in ("chaos", "all"):
            run(
                [
                    *compose,
                    "up",
                    "-d",
                    "--wait",
                    "--wait-timeout",
                    "180",
                    "api",
                    "worker-a",
                    "worker-b",
                    "scheduler-a",
                    "scheduler-b",
                    "dlq",
                ]
            )
            command = [
                "python",
                "-m",
                "pytest",
                "tests/chaos",
                "--run-chaos",
                "-s",
                "-q",
                "--junitxml=artifacts/junit/chaos.xml",
            ]
            if args.case != "all":
                command.extend(["-k", args.case])
            run([*compose, "run", "--rm", "--no-deps", "-T", "runner", *command])
        result = 0
    except (subprocess.CalledProcessError, KeyboardInterrupt) as exc:
        print(
            f"FAIL [test stack] {type(exc).__name__}; see artifacts/test-stack.log", file=sys.stderr
        )
    finally:
        run(
            [*compose, "logs", "--no-color", "--tail=300"],
            check=False,
            output=artifacts / "test-stack.log",
        )
        if not args.keep:
            cleanup = run([*compose, "down", "--volumes", "--remove-orphans"], check=False)
            if cleanup.returncode:
                result = 1
        else:
            print(
                f"Kept test project {project}; use {env_file.relative_to(PROJECT_ROOT)} with "
                "docker-compose.test.yml to inspect or remove it."
            )
    return result


if __name__ == "__main__":
    raise SystemExit(main())
