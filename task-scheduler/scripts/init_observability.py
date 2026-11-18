#!/usr/bin/env python3
"""Generate a local Grafana credential once; preserve existing settings and never print secrets."""

import os
import re
import secrets
from pathlib import Path


def initialize(path: Path = Path(".env")) -> None:
    """Fill only a missing/empty credential, requiring the normal local environment file."""
    content = path.read_text()
    key = "GRAFANA_ADMIN_PASSWORD"
    match = re.search(r"^GRAFANA_ADMIN_PASSWORD=(.*)$", content, re.MULTILINE)
    if os.environ.get(key) or (match and match.group(1).strip().strip("\"'")):
        return
    setting = f"{key}={secrets.token_urlsafe(32)}"
    if match:
        content = content[: match.start()] + setting + content[match.end() :]
    else:
        content = content.rstrip() + "\n" + setting + "\n"
    path.write_text(content)
    path.chmod(0o600)
    print("[OK] Grafana admin credential generated in ignored .env")


if __name__ == "__main__":
    initialize()
