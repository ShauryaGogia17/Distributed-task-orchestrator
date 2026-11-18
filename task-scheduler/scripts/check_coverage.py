#!/usr/bin/env python3
"""Enforce >80% unit line coverage independently for shared/ and Python services/."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def check(path: Path, minimum: float = 81) -> bool:
    """Fail on missing source coverage as well as a percentage below the configured gate."""
    document = json.loads(path.read_text())
    passed = True
    summary = {}
    for prefix in ("shared/", "services/"):
        files = [
            value["summary"]
            for name, value in document["files"].items()
            if name.replace("\\", "/").startswith(prefix)
        ]
        statements = sum(value["num_statements"] for value in files)
        covered = sum(value["covered_lines"] for value in files)
        percent = 100 * covered / statements if statements else 0
        ok = bool(statements) and percent >= minimum
        summary[prefix] = {
            "covered_lines": covered,
            "statements": statements,
            "percent": round(percent, 2),
            "minimum": minimum,
            "passed": ok,
        }
        print(
            f"{'PASS' if ok else 'FAIL'} [unit coverage] {prefix}: {percent:.2f}% "
            f"({covered}/{statements}), required {minimum:g}%"
        )
        passed &= ok
    path.with_name("package-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return passed


def main() -> int:
    """Read the standard coverage JSON artifact and exit nonzero for either failed package."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "report", type=Path, nargs="?", default=Path("artifacts/coverage/coverage.json")
    )
    parser.add_argument("--minimum", type=float, default=81)
    args = parser.parse_args()
    return 0 if check(args.report, args.minimum) else 1


if __name__ == "__main__":
    raise SystemExit(main())
