from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar("T")


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
        os.replace(temp_name, path)
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