#!/usr/bin/env python3
"""Check whether the latest Codex session is still processing a turn."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class SessionStatus:
    session_id: str
    session_file: Path
    archived: bool
    thinking: bool
    last_turn_id: str | None
    last_task_started_at: str | None
    last_task_complete_at: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check whether a Codex session still has an unfinished turn.",
    )
    parser.add_argument(
        "--codex-home",
        default="~/.codex",
        help="Codex home directory (default: ~/.codex)",
    )
    parser.add_argument(
        "--session-id",
        help="Specific session id from session_index.jsonl",
    )
    parser.add_argument(
        "--session-file",
        help="Specific rollout JSONL file to inspect",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def find_latest_session_id(codex_home: Path) -> str:
    index_path = codex_home / "session_index.jsonl"
    if not index_path.exists():
        raise FileNotFoundError(f"session index not found: {index_path}")

    latest_record: dict[str, Any] | None = None
    latest_updated_at: datetime | None = None
    for record in load_jsonl(index_path):
        session_id = record.get("id")
        updated_at = record.get("updated_at")
        if not session_id or not updated_at:
            continue
        updated_dt = parse_iso8601(updated_at)
        if latest_updated_at is None or updated_dt > latest_updated_at:
            latest_updated_at = updated_dt
            latest_record = record

    if not latest_record:
        raise ValueError(f"no usable records in {index_path}")
    return str(latest_record["id"])


def find_session_file(codex_home: Path, session_id: str) -> tuple[Path, bool]:
    sessions_root = codex_home / "sessions"
    archived_root = codex_home / "archived_sessions"

    for root, archived in ((sessions_root, False), (archived_root, True)):
        matches = sorted(root.rglob(f"*{session_id}*.jsonl"))
        if matches:
            return matches[-1], archived
    raise FileNotFoundError(f"unable to locate rollout file for session id {session_id}")


def inspect_session(session_file: Path, session_id: str, archived: bool) -> SessionStatus:
    last_started_turn_id: str | None = None
    last_started_at: str | None = None
    completed_at_by_turn: dict[str, str] = {}

    for item in load_jsonl(session_file):
        timestamp = item.get("timestamp")
        if item.get("type") != "event_msg":
            continue
        payload = item.get("payload")
        if not isinstance(payload, dict):
            continue

        payload_type = payload.get("type")
        turn_id = payload.get("turn_id")
        if payload_type == "task_started" and turn_id:
            last_started_turn_id = str(turn_id)
            last_started_at = str(timestamp) if timestamp else None
        elif payload_type == "task_complete" and turn_id and timestamp:
            completed_at_by_turn[str(turn_id)] = str(timestamp)

    last_completed_at = None
    if last_started_turn_id:
        last_completed_at = completed_at_by_turn.get(last_started_turn_id)

    return SessionStatus(
        session_id=session_id,
        session_file=session_file,
        archived=archived,
        thinking=bool(last_started_turn_id and not last_completed_at),
        last_turn_id=last_started_turn_id,
        last_task_started_at=last_started_at,
        last_task_complete_at=last_completed_at,
    )


def derive_session_id_from_file(session_file: Path) -> str:
    stem = session_file.stem
    match = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$", stem)
    if match:
        return match.group(1)
    return stem


def resolve_status(args: argparse.Namespace) -> SessionStatus:
    codex_home = Path(args.codex_home).expanduser()

    if args.session_file:
        session_file = Path(args.session_file).expanduser()
        if not session_file.exists():
            raise FileNotFoundError(f"session file not found: {session_file}")
        archived = "archived_sessions" in session_file.parts
        session_id = derive_session_id_from_file(session_file)
        return inspect_session(session_file, session_id, archived)

    session_id = args.session_id or find_latest_session_id(codex_home)
    session_file, archived = find_session_file(codex_home, session_id)
    return inspect_session(session_file, session_id, archived)


def print_human(status: SessionStatus) -> None:
    print(f"session id: {status.session_id}")
    print(f"session file: {status.session_file}")
    print(f"archived: {'yes' if status.archived else 'no'}")
    print(f"thinking: {'yes' if status.thinking else 'no'}")
    print(f"last turn id: {status.last_turn_id or '-'}")
    print(f"last task started: {status.last_task_started_at or '-'}")
    print(f"last task complete: {status.last_task_complete_at or '-'}")


def main() -> int:
    args = parse_args()
    try:
        status = resolve_status(args)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(
            json.dumps(
                {
                    "session_id": status.session_id,
                    "session_file": str(status.session_file),
                    "archived": status.archived,
                    "thinking": status.thinking,
                    "last_turn_id": status.last_turn_id,
                    "last_task_started_at": status.last_task_started_at,
                    "last_task_complete_at": status.last_task_complete_at,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print_human(status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
