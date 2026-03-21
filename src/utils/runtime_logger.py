from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class RuntimeLogger:
    logs_dir: Path

    def __post_init__(self) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_log = self.logs_dir / "runtime.log"
        self.events_log = self.logs_dir / "events.jsonl"

    def _ts(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _append_line(self, path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def info(self, message: str) -> None:
        line = f"{self._ts()} INFO {message}"
        print(line)
        self._append_line(self.runtime_log, line)

    def warning(self, message: str) -> None:
        line = f"{self._ts()} WARNING {message}"
        print(line)
        self._append_line(self.runtime_log, line)

    def event(self, event_type: str, payload: dict[str, Any]) -> None:
        row = {
            "ts": self._ts(),
            "event": event_type,
            "payload": payload,
        }
        self._append_line(self.events_log, json.dumps(row, ensure_ascii=False, default=str))
