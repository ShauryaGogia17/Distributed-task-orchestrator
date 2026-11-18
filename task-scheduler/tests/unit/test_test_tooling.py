"""Test infrastructure gates and fault safety without invoking Docker."""

import json
from unittest.mock import AsyncMock

import pytest

from scripts.chaos import DockerFaults, wait_for
from scripts.check_coverage import check


@pytest.mark.parametrize(
    "project", ["task-scheduler", "production", "task-scheduler-test-; rm", ""]
)
def test_fault_controller_rejects_non_test_projects(project: str) -> None:
    """A typo cannot redirect fault injection to the normal development stack."""
    with pytest.raises(ValueError):
        DockerFaults(project)


async def test_fault_controller_rechecks_labels_before_mutation() -> None:
    """Project prefix alone is insufficient: the inspected container must carry the owned label."""
    controller = DockerFaults("task-scheduler-test-unit")
    controller.command = AsyncMock(
        side_effect=[
            "abc\n",
            json.dumps(
                [
                    {
                        "Config": {
                            "Labels": {
                                "com.docker.compose.project": "production",
                                "io.task-scheduler.test-stack": "true",
                            }
                        }
                    }
                ]
            ),
        ]
    )
    with pytest.raises(RuntimeError, match="Refusing"):
        await controller.mutate("kafka", "stop")
    assert controller.command.await_count == 2
    with pytest.raises(ValueError):
        await controller.container("postgres")


async def test_fault_waits_name_missing_invariant() -> None:
    """PASS evidence cannot be emitted after an unbounded or unidentified failed wait."""
    with pytest.raises(TimeoutError, match="heartbeat"):
        await wait_for(AsyncMock(return_value=None), 0.001, "worker heartbeat")


@pytest.mark.parametrize("percent,passed", [(90, True), (80, False), (0, False)])
def test_coverage_gate_checks_each_package_not_only_combined(tmp_path, percent, passed) -> None:
    """High shared coverage must not hide uncovered service code or missing files."""
    report = tmp_path / "coverage.json"
    report.write_text(
        json.dumps(
            {
                "files": {
                    "shared/a.py": {"summary": {"num_statements": 100, "covered_lines": 100}},
                    "services/a.py": {"summary": {"num_statements": 100, "covered_lines": percent}},
                }
            }
        )
    )
    assert check(report) == passed
    assert (tmp_path / "package-summary.json").exists()


@pytest.mark.parametrize("fail", [False, True])
def test_test_stack_uses_per_run_environment_and_always_cleans(monkeypatch, tmp_path, fail) -> None:
    """A concurrent run cannot redirect this invocation's Compose cleanup to another project."""
    import subprocess

    from scripts import test_stack

    calls = []
    monkeypatch.setattr(test_stack, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(test_stack.sys, "argv", ["test_stack.py", "integration", "--no-build"])

    def run(args, **kwargs):
        calls.append(args)
        if fail and "up" in args:
            raise subprocess.CalledProcessError(1, args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(test_stack, "run", run)
    assert test_stack.main() == int(fail)
    assert any("down" in args and "--volumes" in args for args in calls)
    environment_files = {args[args.index("--env-file") + 1] for args in calls}
    assert len(environment_files) == 1
    environment_path = next(iter(environment_files))
    assert not environment_path.endswith("/test-stack.env")
    assert (tmp_path / "artifacts/test-stack.env").read_text() == __import__("pathlib").Path(
        environment_path
    ).read_text()
