"""Minimal JSON logging for Lambdas (no third-party dependencies)."""

import json
import sys
from datetime import UTC, datetime
from typing import Any


def log(event: str, level: str = "info", **fields: Any) -> None:
    line = {"event": event, "level": level, "timestamp": datetime.now(UTC).isoformat(), **fields}
    sys.stdout.write(json.dumps(line, default=str) + "\n")
