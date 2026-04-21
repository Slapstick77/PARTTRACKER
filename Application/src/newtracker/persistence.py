from __future__ import annotations

import errno
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_ATOMIC_REPLACE_ATTEMPTS = 6
_ATOMIC_REPLACE_INITIAL_DELAY_SECONDS = 0.05


def read_json_file(
    path: Path,
    default_factory: Callable[[], T],
    *,
    quarantine_corrupt: bool = False,
) -> T:
    if not path.exists():
        return default_factory()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if quarantine_corrupt:
            quarantine_file(path)
        return default_factory()

    return payload


def _is_retryable_replace_error(exc: OSError) -> bool:
    if isinstance(exc, PermissionError):
        return True
    if getattr(exc, "winerror", None) in {5, 32}:
        return True
    return exc.errno in {errno.EACCES, errno.EPERM}


def _replace_with_retries(source: str, destination: Path) -> None:
    delay_seconds = _ATOMIC_REPLACE_INITIAL_DELAY_SECONDS
    for attempt in range(_ATOMIC_REPLACE_ATTEMPTS):
        try:
            os.replace(source, destination)
            return
        except OSError as exc:
            if attempt >= _ATOMIC_REPLACE_ATTEMPTS - 1 or not _is_retryable_replace_error(exc):
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retries(temp_name, path)
    finally:
        temp_path = Path(temp_name)
        if temp_path.exists():
            temp_path.unlink()


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2), encoding="utf-8")


def quarantine_file(path: Path) -> Path | None:
    if not path.exists():
        return None

    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    target = path.with_name(f"{path.stem}.corrupt-{stamp}{path.suffix}")
    try:
        os.replace(path, target)
    except OSError:
        return None
    return target