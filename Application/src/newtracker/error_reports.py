from __future__ import annotations

import json
import re
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .db import DATA_DIR
from .persistence import atomic_write_text

DEFAULT_ERROR_REPORT_DIR = DATA_DIR / "error_reports"


def default_error_report_directory() -> Path:
    return DEFAULT_ERROR_REPORT_DIR


def resolve_error_report_directory(raw_value: str | None) -> Path:
    configured = str(raw_value or "").strip()
    if configured:
        return Path(configured).expanduser()
    return default_error_report_directory()


def _slugify(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return token or "error"


def _format_json_block(payload: Mapping[str, Any] | None) -> str:
    if not payload:
        return ""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)


def write_error_report(
    *,
    directory: Path,
    category: str,
    summary: str,
    traceback_text: str = "",
    request_info: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Path:
    target_dir = directory.expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    filename = f"{now.strftime('%Y%m%d-%H%M%S')}-{_slugify(category)}-{secrets.token_hex(4)}.log"
    report_path = target_dir / filename

    lines = [
        "NEWTRACKER Error Report",
        f"Timestamp: {now.isoformat(timespec='seconds')}",
        f"Category: {category}",
        f"Summary: {summary}",
    ]

    request_block = _format_json_block(request_info)
    if request_block:
        lines.extend(["", "Request", request_block])

    extra_block = _format_json_block(extra)
    if extra_block:
        lines.extend(["", "Context", extra_block])

    if traceback_text:
        lines.extend(["", "Traceback", traceback_text.rstrip()])

    atomic_write_text(report_path, "\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_path


def list_error_reports(directory: Path, *, limit: int = 8) -> list[dict[str, Any]]:
    target_dir = directory.expanduser()
    if not target_dir.exists() or not target_dir.is_dir():
        return []

    report_files: list[tuple[Path, Any]] = []
    for path in target_dir.iterdir():
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        report_files.append((path, stat))

    report_files.sort(key=lambda item: item[1].st_mtime, reverse=True)

    reports: list[dict[str, Any]] = []
    for path, stat in report_files[:limit]:
        reports.append(
            {
                "name": path.name,
                "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                "size_label": f"{max(1, (int(stat.st_size) + 1023) // 1024)} KB",
            }
        )
    return reports


def resolve_error_report_path(directory: Path, report_name: str) -> Path:
    target_dir = directory.expanduser().resolve()
    candidate = (target_dir / report_name).resolve()
    if candidate.parent != target_dir:
        raise FileNotFoundError(report_name)
    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(report_name)
    return candidate