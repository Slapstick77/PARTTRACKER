from __future__ import annotations

import json
import os
import re
import sqlite3
import traceback
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import DATA_DIR, get_connection, get_database_settings
from .parser import (
    ParsedNestPart,
    file_sha256,
    parse_channel_rollformer_input_csv,
    parse_dat_file,
    parse_nest_comparison_csv,
    parse_order_in_csv,
    parse_spp_label_file_csv,
    parse_yanoprog_csv,
)
from .persistence import atomic_write_json, read_json_file
from .schema import create_schema

SCAN_CACHE_PATH = DATA_DIR / "import_scan_cache.json"
IMPORT_STABILITY_WINDOW = timedelta(minutes=2)
MAX_IMPORT_FILE_AGE = timedelta(days=183)
AMADA_DAT_PARSER_VERSION = "2026-04-14-job-context-v1"

IGNORED_SOURCE_PATHS = {
    Path(r"P:\Manufacturing\CNC\Programming folders\OLD Zipped"),
    Path(r"P:\Manufacturing\CNC\EMK1\plot"),
    Path(r"P:\Manufacturing\CNC\Programming folders\OLD"),
    Path(r"P:\Manufacturing\CNC\Laser\MOMfiles"),
    Path(r"P:\Manufacturing\CNC\Laser\OLD"),
    Path(r"P:\Manufacturing\CNC\EMK1\OLD"),
    Path(r"P:\Manufacturing\CNC\Amada\OLD"),
}
IGNORED_SOURCE_PATH_KEYS = {str(path).casefold() for path in IGNORED_SOURCE_PATHS}

# Folder names always ignored regardless of root (P: drive, /mnt/imports, etc.)
# X15 subfolders inside EMK1/Laser/Amada are resolution-only paths, not regular imports.
IGNORED_SUBFOLDER_NAMES = {
    "x15",
}

IMMUTABLE_SOURCE_ROOTS = {Path(r"P:\Manufacturing\CNC")}
IMMUTABLE_SOURCE_ROOT_KEYS = {str(path).casefold() for path in IMMUTABLE_SOURCE_ROOTS}

ProgressCallback = Callable[[dict[str, Any]], None]
WarningCallback = Callable[[dict[str, Any]], None]


class SourceAccessError(RuntimeError):
    pass


def _db_backend() -> str:
    try:
        return get_database_settings().backend
    except Exception:
        return "sqlite"


def _empty_scan_cache() -> dict[str, Any]:
    return {"roots": {}, "deferred_supported_files": []}


def canonical_key(path: Path) -> str:
    lower_name = path.name.lower()
    if lower_name.endswith("yanoprog.csv"):
        return "*yanoprog.csv"
    if lower_name.startswith("orderin") and lower_name.endswith(".csv"):
        return "orderin*.csv"
    if lower_name.startswith("channelrollformerinput") and lower_name.endswith(".csv"):
        return "channelrollformerinput*.csv"
    if lower_name in {"nestcomparison.csv", "spplabelfile.csv"}:
        return lower_name
    return path.suffix.lower()


def classify_file_name(name: str) -> str | None:
    lower_name = name.lower()
    if lower_name == "nestcomparison.csv":
        return "nest_comparison"
    if lower_name == "spplabelfile.csv":
        return "spp_label_file"
    if lower_name.endswith("yanoprog.csv"):
        return "yanoprog"
    if lower_name.startswith("orderin") and lower_name.endswith(".csv"):
        return "order_in"
    if lower_name.startswith("channelrollformerinput") and lower_name.endswith(".csv"):
        return "channel_rollformer"
    if lower_name.endswith(".dat"):
        return "amada_dat"
    return None


def classify_file(path: Path) -> str | None:
    return classify_file_name(path.name)


def load_scan_cache() -> dict[str, Any]:
    payload = read_json_file(SCAN_CACHE_PATH, _empty_scan_cache, quarantine_corrupt=True)

    if not isinstance(payload, dict):
        return _empty_scan_cache()

    roots = payload.get("roots")
    deferred = payload.get("deferred_supported_files")
    return {
        "roots": dict(roots) if isinstance(roots, Mapping) else {},
        "deferred_supported_files": [str(value) for value in deferred] if isinstance(deferred, list) else [],
    }


def save_scan_cache(cache: dict[str, Any], *, warning_callback: WarningCallback | None = None) -> None:
    payload = {
        "roots": dict(cache.get("roots", {})) if isinstance(cache.get("roots"), Mapping) else {},
        "deferred_supported_files": [str(value) for value in cache.get("deferred_supported_files", [])],
    }
    try:
        atomic_write_json(SCAN_CACHE_PATH, payload)
    except OSError as exc:
        # This cache is only an import optimization. If Windows or another process
        # briefly locks the sidecar file, keep the import running and rebuild the
        # cache on a later pass instead of failing the whole job.
        if warning_callback is not None:
            warning_callback(
                {
                    "category": "import-cache-write-warning",
                    "summary": f"Import scan cache write skipped: {exc}",
                    "traceback_text": traceback.format_exc(),
                    "extra": {
                        "cache_path": str(SCAN_CACHE_PATH),
                        "error_type": type(exc).__name__,
                    },
                }
            )
        return


def clear_scan_cache() -> None:
    save_scan_cache(_empty_scan_cache())


def _raise_if_source_unavailable(path: Path, exc: OSError, *, action: str) -> None:
    if is_immutable_source_path(path):
        raise SourceAccessError(f"Lost access to P drive while {action}: {path}") from exc


def _directory_mtime_ns(path: Path) -> int:
    try:
        stat = path.stat()
    except OSError as exc:
        _raise_if_source_unavailable(path, exc, action="reading source directory")
        raise
    return getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))


def _safe_stat(path: Path):
    try:
        return path.stat()
    except OSError as exc:
        _raise_if_source_unavailable(path, exc, action="reading source file metadata")
        return None


def is_ignored_source_path(path: Path) -> bool:
    candidate = str(path).casefold()
    for ignored in IGNORED_SOURCE_PATH_KEYS:
        if candidate == ignored or candidate.startswith(f"{ignored}\\") or candidate.startswith(f"{ignored}/"):
            return True
    if path.name.casefold() in IGNORED_SUBFOLDER_NAMES:
        return True
    return False


def is_immutable_source_path(path: Path) -> bool:
    candidate = str(path).casefold()
    for root in IMMUTABLE_SOURCE_ROOT_KEYS:
        if candidate == root or candidate.startswith(f"{root}\\"):
            return True
    return False


def should_trust_directory_cache(path: Path, *, recursive: bool, is_root: bool) -> bool:
    if is_root and not recursive and is_immutable_source_path(path):
        return False
    return True


def _normalize_cached_node(node: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(node, Mapping):
        return None
    cached_files = node.get("all_supported_files")
    children = node.get("children")
    return {
        "path": str(node.get("path", "")),
        "recursive": bool(node.get("recursive")),
        "mtime_ns": int(node.get("mtime_ns") or 0),
        "all_supported_files": [str(value) for value in cached_files] if isinstance(cached_files, list) else [],
        "filtered_old_files": int(node.get("filtered_old_files") or 0),
        "children": dict(children) if isinstance(children, Mapping) else {},
    }


def _scan_directory_tree(
    path: Path,
    *,
    recursive: bool,
    now: datetime,
    changed_since: datetime | None,
    processed_paths: set[str],
    deferred_paths: set[str],
    next_deferred: set[str],
    cache_node: Mapping[str, Any] | None,
    root_index: int,
    total_roots: int,
    is_root: bool,
) -> tuple[list[Path], int, dict[str, Any], int]:
    if is_ignored_source_path(path):
        return [], 0, {
            "path": str(path),
            "recursive": recursive,
            "mtime_ns": 0,
            "all_supported_files": [],
            "filtered_old_files": 0,
            "children": {},
        }, 0

    normalized_cache = _normalize_cached_node(cache_node)
    try:
        mtime_ns = _directory_mtime_ns(path)
    except OSError:
        return [], 0, {
            "path": str(path),
            "recursive": recursive,
            "mtime_ns": 0,
            "all_supported_files": [],
            "filtered_old_files": 0,
            "children": {},
        }, 0

    if changed_since is not None and not is_root:
        changed_at = datetime.fromtimestamp(mtime_ns / 1_000_000_000)
        if changed_at < changed_since:
            return [], 0, {
                "path": str(path),
                "recursive": recursive,
                "mtime_ns": mtime_ns,
                "all_supported_files": [],
                "filtered_old_files": 0,
                "children": {},
            }, 0

    if (
        should_trust_directory_cache(path, recursive=recursive, is_root=is_root)
        and normalized_cache
        and normalized_cache["recursive"] == recursive
        and normalized_cache["mtime_ns"] == mtime_ns
    ):
        return (
            [Path(value) for value in normalized_cache["all_supported_files"]],
            normalized_cache["filtered_old_files"],
            normalized_cache,
            0,
        )

    supported_files: list[Path] = []
    filtered_old_files = 0
    unstable_recent_files = 0
    children: dict[str, Any] = {}
    child_cache_map = {}
    if normalized_cache:
        child_cache_map = {
            str(child_path).casefold(): child_node
            for child_path, child_node in normalized_cache.get("children", {}).items()
        }

    try:
        with os.scandir(path) as iterator:
            entries = sorted(iterator, key=lambda entry: entry.name.casefold())
    except OSError as exc:
        _raise_if_source_unavailable(path, exc, action="scanning source directory")
        entries = []

    for entry in entries:
        if entry.is_dir(follow_symlinks=False):
            if not recursive:
                continue
            entry_path = Path(entry.path)
            if is_ignored_source_path(entry_path):
                continue
            child_files, child_filtered_old, child_node, child_unstable = _scan_directory_tree(
                entry_path,
                recursive=True,
                now=now,
                changed_since=changed_since,
                processed_paths=processed_paths,
                deferred_paths=deferred_paths,
                next_deferred=next_deferred,
                cache_node=child_cache_map.get(str(entry_path).casefold()),
                root_index=root_index,
                total_roots=total_roots,
                is_root=False,
            )
            supported_files.extend(child_files)
            filtered_old_files += child_filtered_old
            unstable_recent_files += child_unstable
            children[str(entry_path)] = child_node
            continue

        if not entry.is_file(follow_symlinks=False):
            continue

        file_type = classify_file_name(entry.name)
        if file_type is None:
            continue

        entry_path = Path(entry.path)
        if is_ignored_source_path(entry_path):
            continue

        normalized_path = str(entry_path).casefold()
        immutable_source = is_immutable_source_path(entry_path)
        already_processed = normalized_path in processed_paths

        if immutable_source and already_processed and normalized_path not in deferred_paths:
            continue

        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except OSError as exc:
            _raise_if_source_unavailable(entry_path, exc, action="reading source file metadata")
            continue

        if (
            changed_since is not None
            and entry_stat.st_mtime < changed_since.timestamp()
            and normalized_path not in deferred_paths
            and (not immutable_source or already_processed)
        ):
            continue

        recent_enough = _is_recent_enough_stat(entry_stat, now=now)
        if not recent_enough:
            filtered_old_files += 1
            continue

        if datetime.fromtimestamp(entry_stat.st_mtime) > now - IMPORT_STABILITY_WINDOW:
            unstable_recent_files += 1
            next_deferred.add(str(entry_path))
            continue

        supported_files.append(entry_path)

    node = {
        "path": str(path),
        "recursive": recursive,
        "mtime_ns": mtime_ns,
        "all_supported_files": [str(file_path) for file_path in supported_files],
        "filtered_old_files": filtered_old_files,
        "children": children,
    }
    return supported_files, filtered_old_files, node, unstable_recent_files


def is_recent_enough(path: Path, now: datetime | None = None) -> bool | None:
    stat = _safe_stat(path)
    if stat is None:
        return None
    return _is_recent_enough_stat(stat, now=now)


def _is_recent_enough_stat(stat: os.stat_result, *, now: datetime | None = None) -> bool:
    cutoff = (now or datetime.now()) - MAX_IMPORT_FILE_AGE
    created_at = datetime.fromtimestamp(stat.st_ctime)
    return created_at >= cutoff


def should_scan_recursively(root: Path) -> bool:
    return "programming" in root.name.casefold()


def file_content_fingerprint(path: Path, file_type: str | None = None) -> str:
    resolved_type = file_type or classify_file(path) or "unknown"
    try:
        base_hash = file_sha256(path)
    except OSError as exc:
        _raise_if_source_unavailable(path, exc, action="reading source file contents")
        raise FileNotFoundError(f"File is not readable: {path}") from exc
    if resolved_type == "amada_dat":
        return f"{base_hash}|parser={AMADA_DAT_PARSER_VERSION}"
    return base_hash


def should_process_file(connection: sqlite3.Connection, path: Path) -> bool:
    row = connection.execute(
        "SELECT file_size, modified_time, content_hash, status FROM processed_files WHERE file_path = ?",
        (str(path),),
    ).fetchone()
    if row is not None and row["status"] == "processed" and is_immutable_source_path(path):
        return False

    stat = _safe_stat(path)
    if stat is None:
        raise FileNotFoundError(f"File is no longer available: {path}")
    file_type = classify_file(path)
    if row is None:
        return True

    if row["file_size"] != stat.st_size or float(row["modified_time"]) != stat.st_mtime:
        return True

    expected_fingerprint = file_content_fingerprint(path, file_type)
    return (row["content_hash"] or "") != expected_fingerprint


def load_processed_paths(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["file_path"]).casefold()
        for row in connection.execute(
            "SELECT file_path FROM processed_files WHERE status = 'processed'"
        ).fetchall()
    }


def upsert_processed_file(
    connection: sqlite3.Connection,
    *,
    path: Path,
    file_type: str,
    status: str,
    content_hash: str | None,
    last_error: str | None = None,
) -> None:
    stat = _safe_stat(path)
    if stat is None:
        return
    if _db_backend() == "sqlserver":
        existing = connection.execute(
            "SELECT 1 FROM processed_files WHERE file_path = ?",
            (str(path),),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO processed_files (
                    file_path, file_name, file_type, file_size, modified_time, content_hash, status, last_error, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (str(path), path.name, file_type, stat.st_size, stat.st_mtime, content_hash, status, last_error),
            )
            return

        connection.execute(
            """
            UPDATE processed_files
            SET file_name = ?,
                file_type = ?,
                file_size = ?,
                modified_time = ?,
                content_hash = ?,
                status = ?,
                last_error = ?,
                processed_at = CURRENT_TIMESTAMP
            WHERE file_path = ?
            """,
            (path.name, file_type, stat.st_size, stat.st_mtime, content_hash, status, last_error, str(path)),
        )
        return

    connection.execute(
        """
        INSERT INTO processed_files (
            file_path, file_name, file_type, file_size, modified_time, content_hash, status, last_error, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(file_path) DO UPDATE SET
            file_name = excluded.file_name,
            file_type = excluded.file_type,
            file_size = excluded.file_size,
            modified_time = excluded.modified_time,
            content_hash = excluded.content_hash,
            status = excluded.status,
            last_error = excluded.last_error,
            processed_at = CURRENT_TIMESTAMP
        """,
        (str(path), path.name, file_type, stat.st_size, stat.st_mtime, content_hash, status, last_error),
    )


def normalize_revision(value: str | None) -> str:
    cleaned = (value or "").strip()
    if cleaned in {"", "-"}:
        return ""
    return cleaned


def _upsert_part_attribute_row(connection: sqlite3.Connection, row: Any) -> None:
    params = (
        row.com_number,
        row.com_number or "",
        row.part_number,
        row.rev_level,
        row.rev_level or "",
        normalize_revision(row.rev_level),
        row.build_date,
        row.build_date or "",
        row.quantity_per,
        row.nested_on,
        row.length,
        row.width,
        row.thickness,
        row.item_class,
        row.department_number,
        row.part_parent,
        row.ops_files,
        row.pair_part_number,
        row.p4_edits,
        row.collection_cart,
        row.routing,
        row.model_number,
        row.shear,
        row.punch,
        row.form,
        row.requires_forming,
        row.weight,
        row.coded_part_msg,
        row.parent_model_number,
        row.skid_number,
        row.page_number,
        row.split_value,
        row.source_file_path,
    )
    if _db_backend() != "sqlserver":
        connection.execute(
            """
            INSERT INTO part_attributes (
                com_number, com_number_key, part_number, rev_level, rev_level_key, normalized_rev_key, build_date, build_date_key,
                quantity_per, nested_on, length, width, thickness, item_class, department_number, part_parent,
                ops_files, pair_part_number, p4_edits, collection_cart, routing, model_number, shear, punch,
                form, requires_forming, weight, coded_part_msg, parent_model_number, skid_number, page_number,
                split_value, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(com_number_key, part_number, rev_level_key, build_date_key) DO UPDATE SET
                normalized_rev_key = excluded.normalized_rev_key,
                quantity_per = excluded.quantity_per,
                nested_on = excluded.nested_on,
                length = excluded.length,
                width = excluded.width,
                thickness = excluded.thickness,
                item_class = excluded.item_class,
                department_number = excluded.department_number,
                part_parent = excluded.part_parent,
                ops_files = excluded.ops_files,
                pair_part_number = excluded.pair_part_number,
                p4_edits = excluded.p4_edits,
                collection_cart = excluded.collection_cart,
                routing = excluded.routing,
                model_number = excluded.model_number,
                shear = excluded.shear,
                punch = excluded.punch,
                form = excluded.form,
                requires_forming = excluded.requires_forming,
                weight = excluded.weight,
                coded_part_msg = excluded.coded_part_msg,
                parent_model_number = excluded.parent_model_number,
                skid_number = excluded.skid_number,
                page_number = excluded.page_number,
                split_value = excluded.split_value,
                source_file_path = excluded.source_file_path
            """,
            params,
        )
        return

    existing = connection.execute(
        """
        SELECT id FROM part_attributes
        WHERE com_number_key = ? AND part_number = ? AND rev_level_key = ? AND build_date_key = ?
        """,
        (row.com_number or "", row.part_number, row.rev_level or "", row.build_date or ""),
    ).fetchone()
    if existing is None:
        connection.execute(
            """
            INSERT INTO part_attributes (
                com_number, com_number_key, part_number, rev_level, rev_level_key, normalized_rev_key, build_date, build_date_key,
                quantity_per, nested_on, length, width, thickness, item_class, department_number, part_parent,
                ops_files, pair_part_number, p4_edits, collection_cart, routing, model_number, shear, punch,
                form, requires_forming, weight, coded_part_msg, parent_model_number, skid_number, page_number,
                split_value, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        return

    connection.execute(
        """
        UPDATE part_attributes
        SET normalized_rev_key = ?,
            quantity_per = ?,
            nested_on = ?,
            length = ?,
            width = ?,
            thickness = ?,
            item_class = ?,
            department_number = ?,
            part_parent = ?,
            ops_files = ?,
            pair_part_number = ?,
            p4_edits = ?,
            collection_cart = ?,
            routing = ?,
            model_number = ?,
            shear = ?,
            punch = ?,
            form = ?,
            requires_forming = ?,
            weight = ?,
            coded_part_msg = ?,
            parent_model_number = ?,
            skid_number = ?,
            page_number = ?,
            split_value = ?,
            source_file_path = ?
        WHERE id = ?
        """,
        (
            normalize_revision(row.rev_level),
            row.quantity_per,
            row.nested_on,
            row.length,
            row.width,
            row.thickness,
            row.item_class,
            row.department_number,
            row.part_parent,
            row.ops_files,
            row.pair_part_number,
            row.p4_edits,
            row.collection_cart,
            row.routing,
            row.model_number,
            row.shear,
            row.punch,
            row.form,
            row.requires_forming,
            row.weight,
            row.coded_part_msg,
            row.parent_model_number,
            row.skid_number,
            row.page_number,
            row.split_value,
            row.source_file_path,
            int(existing["id"]),
        ),
    )

def _coalesce_text_values(values: list[str]) -> str:
    unique_values: list[str] = []
    for value in values:
        cleaned = (value or "").strip()
        if cleaned and cleaned not in unique_values:
            unique_values.append(cleaned)
    return " | ".join(unique_values)

def aggregate_nest_parts(parts: list[ParsedNestPart]) -> list[ParsedNestPart]:
    grouped: dict[tuple[str, str], list[ParsedNestPart]] = defaultdict(list)
    ordered_keys: list[tuple[str, str]] = []

    for part in parts:
        key = (part.part_number, normalize_revision(part.part_revision))
        if key not in grouped:
            ordered_keys.append(key)
        grouped[key].append(part)

    merged_parts: list[ParsedNestPart] = []
    for key in ordered_keys:
        rows = grouped[key]
        first = rows[0]
        if len(rows) == 1:
            merged_parts.append(first)
            continue

        merged_parts.append(
            replace(
                first,
                quantity_nested=sum(int(row.quantity_nested or 0) for row in rows),
                order_number_raw=_coalesce_text_values([row.order_number_raw for row in rows]),
            )
        )

    return merged_parts


def _normalize_date_token(value: str | None) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    return "".join(ch for ch in cleaned if ch.isdigit())


def _program_date_prefix(value: str | None) -> str:
    token = _normalize_date_token(value)
    return token[:8] if len(token) >= 8 else ""


def _row_value(row: sqlite3.Row, *keys: str) -> str:
    for key in keys:
        if key in row.keys() and row[key] is not None:
            return str(row[key]).strip()
    return ""


def _job_metadata_from_folder_name(folder_name: str) -> tuple[str, str, str]:
    cleaned = folder_name.strip()
    match = re.match(r"^(\d+)\s*(?:\(([^)]*)\))?\s*(.*)$", cleaned)
    if not match:
        return "", "", cleaned
    return match.group(1) or "", (match.group(2) or "").strip(), (match.group(3) or "").strip()


def _job_folder_path_for_file(path: Path, roots: list[Path]) -> Path | None:
    best_root: Path | None = None
    best_length = -1
    for root in roots:
        try:
            path.relative_to(root)
        except ValueError:
            continue
        if len(root.parts) > best_length:
            best_root = root
            best_length = len(root.parts)

    if best_root is None:
        return None

    relative = path.relative_to(best_root)
    if not relative.parts:
        return None
    if len(relative.parts) == 1:
        return path.parent
    return best_root / relative.parts[0]


def _find_source_root(folder_path: Path, roots: list[Path]) -> Path:
    for root in roots:
        try:
            folder_path.relative_to(root)
            return root
        except ValueError:
            continue
    return folder_path.parent


def _get_or_create_job_folder(connection: sqlite3.Connection, folder_path: Path, source_root: Path) -> int:
    folder_name = folder_path.name
    com_number, build_date_code, project_name = _job_metadata_from_folder_name(folder_name)
    existing = connection.execute(
        "SELECT id FROM job_folders WHERE folder_path = ?",
        (str(folder_path),),
    ).fetchone()
    if existing:
        connection.execute(
            """
            UPDATE job_folders
            SET folder_name = ?, com_number = ?, build_date_code = ?, project_name = ?, source_root = ?, last_seen_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (folder_name, com_number or None, build_date_code or None, project_name or None, str(source_root), existing["id"]),
        )
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO job_folders (folder_name, folder_path, com_number, build_date_code, project_name, source_root, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (folder_name, str(folder_path), com_number or None, build_date_code or None, project_name or None, str(source_root)),
    )
    if cursor.lastrowid is None:
        raise RuntimeError(f"Failed to create job folder for {folder_path}")
    return int(cursor.lastrowid)


def _candidate_metadata_from_rows(candidates: list[sqlite3.Row]) -> tuple[int, str | None, str | None]:
    build_dates: list[str] = []
    com_numbers: list[str] = []
    for candidate in candidates:
        build_date = _row_value(candidate, "job_build_date", "build_date_code", "build_day", "build_date")
        com_number = _row_value(candidate, "com_number", "unit_id")
        if build_date and build_date not in build_dates:
            build_dates.append(build_date)
        if com_number and com_number not in com_numbers:
            com_numbers.append(com_number)
    return len(candidates), json.dumps(build_dates) if build_dates else None, json.dumps(com_numbers) if com_numbers else None


def _count_context_matches(connection: sqlite3.Connection, part_numbers: list[str]) -> int:
    normalized = sorted({(part or "").strip() for part in part_numbers if (part or "").strip()})
    if not normalized:
        return 0
    placeholders = ",".join("?" for _ in normalized)
    job_row = connection.execute(
        f"SELECT COUNT(DISTINCT part_number) AS cnt FROM job_parts WHERE part_number IN ({placeholders})",
        normalized,
    ).fetchone()
    attr_row = connection.execute(
        f"SELECT COUNT(DISTINCT part_number) AS cnt FROM part_attributes WHERE part_number IN ({placeholders})",
        normalized,
    ).fetchone()
    return int(job_row["cnt"] or 0) + int(attr_row["cnt"] or 0)


def _select_best_dat_candidate(connection: sqlite3.Connection, candidates: list[Path]) -> tuple[Path, str]:
    scored: list[tuple[tuple[int, int, int, int, str], Path]] = []
    for path in candidates:
        try:
            parsed = parse_dat_file(path)
        except OSError as exc:
            _raise_if_source_unavailable(path, exc, action="reading DAT file")
            continue
        quantities = [max(0, int(part.quantity_nested)) for part in parsed.parts]
        total_quantity = sum(quantities)
        unique_parts = len({(part.part_number or "").strip() for part in parsed.parts if (part.part_number or "").strip()})
        match_count = _count_context_matches(connection, [part.part_number for part in parsed.parts])
        score = (match_count, total_quantity, unique_parts, len(parsed.parts), str(path).casefold())
        scored.append((score, path))

    if not scored:
        raise FileNotFoundError("No readable DAT candidates remained for this barcode filename.")

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_path = scored[0]
    reason = (
        f"match_count={best_score[0]}, total_quantity={best_score[1]}, "
        f"unique_parts={best_score[2]}, part_rows={best_score[3]}"
    )
    return best_path, reason


def _current_program_source(connection: sqlite3.Connection, barcode_filename: str) -> str | None:
    row = connection.execute(
        "SELECT source_file_path FROM program_nests WHERE UPPER(barcode_filename) = UPPER(?)",
        (barcode_filename,),
    ).fetchone()
    return row["source_file_path"] if row else None


def _same_path(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.casefold() == b.casefold()


def import_dat_file(connection: sqlite3.Connection, path: Path) -> int:
    parsed = parse_dat_file(path)
    merged_parts = aggregate_nest_parts(parsed.parts)
    connection.execute("DELETE FROM nest_parts WHERE source_file_path = ?", (str(path),))
    connection.execute("DELETE FROM program_nests WHERE source_file_path = ?", (str(path),))
    connection.execute(
        "DELETE FROM program_nests WHERE barcode_filename = ? AND source_file_path <> ?",
        (parsed.barcode_filename, str(path)),
    )

    cursor = connection.execute(
        """
        INSERT INTO program_nests (
            barcode_filename, program_file_name, program_number, machine_type, sheet_program_name,
            material_code, sheet_length, sheet_width, program_date, program_time, process_count,
            order_number_raw, order_process_code, build_date_code, source_file_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed.barcode_filename,
            parsed.program_file_name,
            parsed.program_number,
            parsed.machine_type,
            parsed.sheet_program_name,
            parsed.material_code,
            parsed.sheet_length,
            parsed.sheet_width,
            parsed.program_date,
            parsed.program_time,
            parsed.process_count,
            parsed.order_number_raw,
            parsed.order_process_code,
            parsed.build_date_code,
            parsed.source_file_path,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError(f"Failed to create program nest for {path}")
    nest_id = int(cursor.lastrowid)

    for part in merged_parts:
        connection.execute(
            """
            INSERT INTO nest_parts (
                nest_id, part_number, part_revision, part_revision_key, quantity_nested,
                order_number_raw, npt_sequence, npt_quantity, npt_rotation, npt_operation,
                npt_x, npt_y, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                nest_id,
                part.part_number,
                part.part_revision,
                normalize_revision(part.part_revision),
                part.quantity_nested,
                part.order_number_raw,
                part.npt_sequence,
                part.npt_quantity,
                part.npt_rotation,
                part.npt_operation,
                part.npt_x,
                part.npt_y,
                parsed.source_file_path,
            ),
        )

    return nest_id


def import_nest_comparison(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> None:
    connection.execute("DELETE FROM part_attributes WHERE source_file_path = ?", (str(path),))
    connection.execute(
        "UPDATE resolved_nest_parts SET matched_job_part_id = NULL WHERE matched_job_part_id IN (SELECT id FROM job_parts WHERE source_file_path = ?)",
        (str(path),),
    )
    connection.execute("DELETE FROM job_parts WHERE source_file_path = ?", (str(path),))
    folder_path = _job_folder_path_for_file(path, roots)
    job_folder_id: int | None = None
    if folder_path is not None:
        job_folder_id = _get_or_create_job_folder(connection, folder_path, _find_source_root(folder_path, roots))

    for row in parse_nest_comparison_csv(path):
        _upsert_part_attribute_row(connection, row)
        if job_folder_id is not None:
            connection.execute(
                """
                INSERT INTO job_parts (
                    job_folder_id, source_type, part_number, revision, revision_key, build_date_code,
                    order_number_raw, process_code, routing, nested_on, quantity, source_file_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_folder_id,
                    "nest_comparison",
                    row.part_number,
                    row.rev_level,
                    normalize_revision(row.rev_level),
                    row.build_date,
                    row.com_number,
                    None,
                    row.routing,
                    row.nested_on,
                    row.quantity_per,
                    row.source_file_path,
                ),
            )


def import_yanoprog(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> None:
    connection.execute(
        "UPDATE resolved_nest_parts SET matched_job_part_id = NULL WHERE matched_job_part_id IN (SELECT id FROM job_parts WHERE source_file_path = ?)",
        (str(path),),
    )
    connection.execute("DELETE FROM job_parts WHERE source_file_path = ?", (str(path),))
    folder_path = _job_folder_path_for_file(path, roots)
    if folder_path is None:
        return
    job_folder_id = _get_or_create_job_folder(connection, folder_path, _find_source_root(folder_path, roots))
    for row in parse_yanoprog_csv(path):
        connection.execute(
            """
            INSERT INTO job_parts (
                job_folder_id, source_type, part_number, revision, revision_key, build_date_code,
                order_number_raw, process_code, routing, nested_on, quantity, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_folder_id,
                "yanoprog",
                row.part_number,
                row.revision,
                normalize_revision(row.revision),
                row.order_date_token,
                row.order_number,
                row.process_code,
                row.route_hint,
                None,
                row.order_quantity,
                row.source_file_path,
            ),
        )


def import_spp_label_file(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> None:
    connection.execute("DELETE FROM job_labels WHERE source_file_path = ?", (str(path),))
    folder_path = _job_folder_path_for_file(path, roots)
    if folder_path is None:
        return
    job_folder_id = _get_or_create_job_folder(connection, folder_path, _find_source_root(folder_path, roots))
    for row in parse_spp_label_file_csv(path):
        connection.execute(
            """
            INSERT INTO job_labels (
                job_folder_id, part_number, barcode, assembly, unit_id, build_day, nest_name,
                material, routing, quantity, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_folder_id,
                row.part_number,
                row.barcode,
                row.assembly,
                row.unit_id,
                row.build_day,
                row.nest_name,
                row.material,
                row.routing,
                row.quantity,
                row.source_file_path,
            ),
        )


def import_order_in(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> None:
    connection.execute("DELETE FROM job_orders WHERE source_file_path = ?", (str(path),))
    folder_path = _job_folder_path_for_file(path, roots)
    if folder_path is None:
        return
    job_folder_id = _get_or_create_job_folder(connection, folder_path, _find_source_root(folder_path, roots))
    for row in parse_order_in_csv(path):
        connection.execute(
            """
            INSERT INTO job_orders (
                job_folder_id, source_type, part_number, material, profile, order_group,
                raw_identifier, quantity, length, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_folder_id,
                "order_in",
                row.part_number,
                row.material,
                row.profile,
                row.order_group,
                row.raw_identifier,
                row.quantity,
                row.length,
                row.source_file_path,
            ),
        )


def import_channel_rollformer(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> None:
    connection.execute("DELETE FROM job_orders WHERE source_file_path = ?", (str(path),))
    folder_path = _job_folder_path_for_file(path, roots)
    if folder_path is None:
        return
    job_folder_id = _get_or_create_job_folder(connection, folder_path, _find_source_root(folder_path, roots))
    row = parse_channel_rollformer_input_csv(path)
    if row is None:
        return
    connection.execute(
        """
        INSERT INTO job_orders (
            job_folder_id, source_type, part_number, material, profile, order_group,
            raw_identifier, quantity, length, source_file_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_folder_id,
            "channel_rollformer",
            None,
            row.material,
            row.profile,
            row.job_number,
            row.description,
            None,
            None,
            row.source_file_path,
        ),
    )


def import_file(connection: sqlite3.Connection, path: Path, roots: list[Path]) -> int | None:
    file_type = classify_file(path)
    if file_type is None:
        return None

    content_hash = file_content_fingerprint(path, file_type)
    imported_nest_id: int | None = None
    try:
        if file_type == "amada_dat":
            imported_nest_id = import_dat_file(connection, path)
        elif file_type == "nest_comparison":
            import_nest_comparison(connection, path, roots)
        elif file_type == "yanoprog":
            import_yanoprog(connection, path, roots)
        elif file_type == "spp_label_file":
            import_spp_label_file(connection, path, roots)
        elif file_type == "order_in":
            import_order_in(connection, path, roots)
        elif file_type == "channel_rollformer":
            import_channel_rollformer(connection, path, roots)
        upsert_processed_file(connection, path=path, file_type=file_type, status="processed", content_hash=content_hash)
    except Exception as exc:
        upsert_processed_file(
            connection,
            path=path,
            file_type=file_type,
            status="error",
            content_hash=content_hash,
            last_error=str(exc),
        )
        raise

    return imported_nest_id


def _build_resolution_cache(connection: sqlite3.Connection) -> dict[str, Any]:
    job_folders_by_id = {
        int(row["id"]): row
        for row in connection.execute("SELECT * FROM job_folders ORDER BY id").fetchall()
    }

    job_parts_by_part_number: dict[str, list[sqlite3.Row]] = defaultdict(list)
    job_parts_by_job_and_part: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
    for row in connection.execute(
        """
        SELECT jp.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name
        FROM job_parts jp
        JOIN job_folders jf ON jf.id = jp.job_folder_id
        ORDER BY jp.id
        """
    ).fetchall():
        part_number = str(row["part_number"] or "")
        job_folder_id = int(row["job_folder_id"])
        job_parts_by_part_number[part_number].append(row)
        job_parts_by_job_and_part[(job_folder_id, part_number)].append(row)

    job_labels_by_part_number: dict[str, list[sqlite3.Row]] = defaultdict(list)
    job_labels_by_job_and_part: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
    for row in connection.execute(
        """
        SELECT jl.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name
        FROM job_labels jl
        JOIN job_folders jf ON jf.id = jl.job_folder_id
        ORDER BY jl.id
        """
    ).fetchall():
        part_number = str(row["part_number"] or "")
        job_folder_id = int(row["job_folder_id"])
        job_labels_by_part_number[part_number].append(row)
        job_labels_by_job_and_part[(job_folder_id, part_number)].append(row)

    part_attributes_by_part_number: dict[str, list[sqlite3.Row]] = defaultdict(list)
    part_attributes_by_part_and_com: dict[tuple[str, Any], list[sqlite3.Row]] = defaultdict(list)
    for row in connection.execute(
        """
        SELECT id, com_number, part_number, form, requires_forming, nested_on, build_date, normalized_rev_key
        FROM part_attributes
        ORDER BY id
        """
    ).fetchall():
        part_number = str(row["part_number"] or "")
        com_number = row["com_number"]
        part_attributes_by_part_number[part_number].append(row)
        part_attributes_by_part_and_com[(part_number, com_number)].append(row)

    return {
        "job_folders_by_id": job_folders_by_id,
        "job_parts_by_part_number": job_parts_by_part_number,
        "job_parts_by_job_and_part": job_parts_by_job_and_part,
        "job_labels_by_part_number": job_labels_by_part_number,
        "job_labels_by_job_and_part": job_labels_by_job_and_part,
        "part_attributes_by_part_number": part_attributes_by_part_number,
        "part_attributes_by_part_and_com": part_attributes_by_part_and_com,
    }


def _resolve_with_part_attributes(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    resolution_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_rev = normalize_revision(row["part_revision"])
    build_date = row["build_date_code"] or ""
    program_date_prefix = _program_date_prefix(row["program_date"])
    cached_candidates = None
    if resolution_cache is not None:
        cached_candidates = resolution_cache["part_attributes_by_part_number"].get(str(row["part_number"] or ""), [])

    if cached_candidates is not None:
        candidates = [
            candidate
            for candidate in cached_candidates
            if candidate["build_date"] == build_date and candidate["normalized_rev_key"] == normalized_rev
        ]
    else:
        candidates = connection.execute(
            """
            SELECT id, com_number, form, requires_forming, nested_on, build_date, normalized_rev_key
            FROM part_attributes
            WHERE part_number = ? AND build_date = ? AND normalized_rev_key = ?
            ORDER BY id
            """,
            (row["part_number"], build_date, normalized_rev),
        ).fetchall()

    resolution_status = "missing_attributes"
    resolution_rule = None
    matched = None
    selected_candidates = candidates

    if candidates:
        matched = candidates[0]
        resolution_status = "resolved"
        resolution_rule = "part+build+normalized_rev"
    else:
        if cached_candidates is not None:
            candidates = [candidate for candidate in cached_candidates if candidate["build_date"] == build_date]
        else:
            candidates = connection.execute(
                """
                SELECT id, com_number, form, requires_forming, nested_on, build_date, normalized_rev_key
                FROM part_attributes
                WHERE part_number = ? AND build_date = ?
                ORDER BY id
                """,
                (row["part_number"], build_date),
            ).fetchall()
        selected_candidates = candidates
        if len(candidates) == 1:
            matched = candidates[0]
            resolution_status = "resolved"
            resolution_rule = "part+build"
        elif len(candidates) > 1:
            matched = candidates[0]
            resolution_status = "partial"
            resolution_rule = "part+build_ambiguous"
        else:
            if cached_candidates is not None:
                candidates = list(cached_candidates)
            else:
                candidates = connection.execute(
                    """
                    SELECT id, com_number, form, requires_forming, nested_on, build_date, normalized_rev_key
                    FROM part_attributes
                    WHERE part_number = ?
                    ORDER BY id
                    """,
                    (row["part_number"],),
                ).fetchall()
            selected_candidates = candidates
            if len(candidates) == 1:
                matched = candidates[0]
                resolution_status = "partial"
                resolution_rule = "part_only"
            elif len(candidates) > 1:
                dated_candidates = []
                if program_date_prefix:
                    dated_candidates = [
                        candidate
                        for candidate in candidates
                        if _normalize_date_token(candidate["build_date"]).startswith(program_date_prefix)
                    ]
                if len(dated_candidates) == 1:
                    matched = dated_candidates[0]
                    selected_candidates = dated_candidates
                    resolution_status = "resolved"
                    resolution_rule = "part+program_date"
                elif len(dated_candidates) > 1:
                    matched = dated_candidates[0]
                    selected_candidates = dated_candidates
                    resolution_status = "partial"
                    resolution_rule = "part+program_date_ambiguous"
                else:
                    matched = candidates[0]
                    resolution_status = "partial"
                    resolution_rule = "part_only_ambiguous"

    match_candidate_count, match_build_dates, match_com_numbers = _candidate_metadata_from_rows(selected_candidates)
    return {
        "com_number": matched["com_number"] if matched else None,
        "form_value": matched["form"] if matched else None,
        "requires_forming": matched["requires_forming"] if matched else None,
        "nested_on": matched["nested_on"] if matched else None,
        "resolution_status": resolution_status,
        "resolution_rule": resolution_rule,
        "match_candidate_count": match_candidate_count,
        "match_build_dates": match_build_dates,
        "match_com_numbers": match_com_numbers,
        "matched_part_attribute_id": matched["id"] if matched else None,
        "matched_job_folder_id": None,
        "matched_job_part_id": None,
        "job_match_score": 0,
        "evidence_summary": None,
    }


def _collect_job_candidates(
    connection: sqlite3.Connection,
    nest_rows: list[sqlite3.Row],
    resolution_cache: dict[str, Any] | None = None,
) -> tuple[int | None, dict[int, dict[str, Any]]]:
    candidates: dict[int, dict[str, Any]] = {}

    def ensure(job_row: sqlite3.Row) -> dict[str, Any]:
        job_id = int(job_row["job_folder_id"])
        info = candidates.get(job_id)
        if info is None:
            info = {
                "job_row": job_row,
                "score": 0,
                "part_hits": set(),
                "label_hits": set(),
                "build_hits": 0,
                "order_hits": 0,
            }
            candidates[job_id] = info
        return info

    for nest_row in nest_rows:
        if resolution_cache is not None:
            part_rows = resolution_cache["job_parts_by_part_number"].get(str(nest_row["part_number"] or ""), [])
            label_rows = resolution_cache["job_labels_by_part_number"].get(str(nest_row["part_number"] or ""), [])
        else:
            part_rows = connection.execute(
                """
                SELECT jp.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name
                FROM job_parts jp
                JOIN job_folders jf ON jf.id = jp.job_folder_id
                WHERE jp.part_number = ?
                ORDER BY jp.id
                """,
                (nest_row["part_number"],),
            ).fetchall()
            label_rows = connection.execute(
                """
                SELECT jl.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name, jl.job_folder_id
                FROM job_labels jl
                JOIN job_folders jf ON jf.id = jl.job_folder_id
                WHERE jl.part_number = ?
                ORDER BY jl.id
                """,
                (nest_row["part_number"],),
            ).fetchall()

        for job_row in part_rows:
            info = ensure(job_row)
            if nest_row["part_number"] not in info["part_hits"]:
                info["score"] += 12
                info["part_hits"].add(nest_row["part_number"])
            if normalize_revision(nest_row["part_revision"]) and normalize_revision(nest_row["part_revision"]) == normalize_revision(job_row["revision"]):
                info["score"] += 2
            nest_build = _row_value(nest_row, "build_date_code")
            job_build = _row_value(job_row, "build_date_code", "job_build_date")
            if nest_build and job_build and _normalize_date_token(nest_build) == _normalize_date_token(job_build):
                info["score"] += 8
                info["build_hits"] += 1
            elif _program_date_prefix(nest_row["program_date"]) and job_build and _normalize_date_token(job_build).startswith(_program_date_prefix(nest_row["program_date"])):
                info["score"] += 4
            if nest_row["order_number_raw"] and _row_value(job_row, "order_number_raw") and nest_row["order_number_raw"].strip().casefold() == _row_value(job_row, "order_number_raw").casefold():
                info["score"] += 8
                info["order_hits"] += 1

        for label_row in label_rows:
            info = ensure(label_row)
            if nest_row["part_number"] not in info["label_hits"]:
                info["score"] += 7
                info["label_hits"].add(nest_row["part_number"])
            nest_build = _row_value(nest_row, "build_date_code")
            label_build = _row_value(label_row, "build_day", "job_build_date")
            if nest_build and label_build and _normalize_date_token(nest_build) == _normalize_date_token(label_build):
                info["score"] += 4
            if _row_value(label_row, "unit_id") and _row_value(label_row, "com_number") and _row_value(label_row, "unit_id").casefold() == _row_value(label_row, "com_number").casefold():
                info["score"] += 2

    if not candidates:
        return None, candidates

    ranked = sorted(
        candidates.items(),
        key=lambda item: (
            item[1]["score"],
            len(item[1]["part_hits"]),
            len(item[1]["label_hits"]),
            item[1]["build_hits"],
            item[1]["order_hits"],
            _row_value(item[1]["job_row"], "com_number"),
            int(item[0]),
        ),
        reverse=True,
    )
    return int(ranked[0][0]), candidates


def _select_best_job_part(
    connection: sqlite3.Connection,
    nest_row: sqlite3.Row,
    job_folder_id: int,
    resolution_cache: dict[str, Any] | None = None,
) -> sqlite3.Row | None:
    if resolution_cache is not None:
        candidates = resolution_cache["job_parts_by_job_and_part"].get((job_folder_id, str(nest_row["part_number"] or "")), [])
    else:
        candidates = connection.execute(
            """
            SELECT jp.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name
            FROM job_parts jp
            JOIN job_folders jf ON jf.id = jp.job_folder_id
            WHERE jp.job_folder_id = ? AND jp.part_number = ?
            ORDER BY jp.id
            """,
            (job_folder_id, nest_row["part_number"]),
        ).fetchall()
    if not candidates:
        return None

    nest_rev = normalize_revision(nest_row["part_revision"])
    nest_build = _row_value(nest_row, "build_date_code")
    program_prefix = _program_date_prefix(nest_row["program_date"])
    nest_order = _row_value(nest_row, "order_number_raw")

    def score(candidate: sqlite3.Row) -> tuple[int, int, int, int]:
        score_value = 10
        if nest_rev and nest_rev == normalize_revision(candidate["revision"]):
            score_value += 4
        candidate_build = _row_value(candidate, "build_date_code", "job_build_date")
        if nest_build and candidate_build and _normalize_date_token(nest_build) == _normalize_date_token(candidate_build):
            score_value += 8
        elif program_prefix and candidate_build and _normalize_date_token(candidate_build).startswith(program_prefix):
            score_value += 3
        if nest_order and _row_value(candidate, "order_number_raw") and nest_order.casefold() == _row_value(candidate, "order_number_raw").casefold():
            score_value += 6
        return score_value, int(candidate["quantity"] or 0), 1 if candidate["source_type"] == "nest_comparison" else 0, int(candidate["id"])

    return sorted(candidates, key=score, reverse=True)[0]


def _select_best_label(
    connection: sqlite3.Connection,
    nest_row: sqlite3.Row,
    job_folder_id: int,
    resolution_cache: dict[str, Any] | None = None,
) -> sqlite3.Row | None:
    if resolution_cache is not None:
        candidates = resolution_cache["job_labels_by_job_and_part"].get((job_folder_id, str(nest_row["part_number"] or "")), [])
    else:
        candidates = connection.execute(
            """
            SELECT jl.*, jf.com_number, jf.build_date_code AS job_build_date, jf.folder_name
            FROM job_labels jl
            JOIN job_folders jf ON jf.id = jl.job_folder_id
            WHERE jl.job_folder_id = ? AND jl.part_number = ?
            ORDER BY jl.id
            """,
            (job_folder_id, nest_row["part_number"]),
        ).fetchall()
    if not candidates:
        return None

    nest_build = _row_value(nest_row, "build_date_code")
    program_prefix = _program_date_prefix(nest_row["program_date"])

    def score(candidate: sqlite3.Row) -> tuple[int, int, int]:
        score_value = 6
        candidate_build = _row_value(candidate, "build_day", "job_build_date")
        if nest_build and candidate_build and _normalize_date_token(nest_build) == _normalize_date_token(candidate_build):
            score_value += 6
        elif program_prefix and candidate_build and _normalize_date_token(candidate_build).startswith(program_prefix):
            score_value += 2
        return score_value, int(candidate["quantity"] or 0), int(candidate["id"])

    return sorted(candidates, key=score, reverse=True)[0]


def _select_attribute_for_job(
    connection: sqlite3.Connection,
    nest_row: sqlite3.Row,
    job_row: sqlite3.Row,
    resolution_cache: dict[str, Any] | None = None,
) -> sqlite3.Row | None:
    if resolution_cache is not None:
        candidates = resolution_cache["part_attributes_by_part_and_com"].get(
            (str(nest_row["part_number"] or ""), job_row["com_number"]),
            [],
        )
    else:
        candidates = connection.execute(
            """
            SELECT id, com_number, form, requires_forming, nested_on, build_date, normalized_rev_key
            FROM part_attributes
            WHERE part_number = ? AND com_number = ?
            ORDER BY id
            """,
            (nest_row["part_number"], job_row["com_number"]),
        ).fetchall()
    if not candidates:
        return None

    nest_rev = normalize_revision(nest_row["part_revision"])
    job_build = _row_value(job_row, "build_date_code", "job_build_date")

    def score(candidate: sqlite3.Row) -> tuple[int, int]:
        score_value = 5
        if nest_rev and nest_rev == normalize_revision(candidate["normalized_rev_key"]):
            score_value += 4
        if job_build and _row_value(candidate, "build_date") and _normalize_date_token(job_build) == _normalize_date_token(candidate["build_date"]):
            score_value += 5
        return score_value, int(candidate["id"])

    return sorted(candidates, key=score, reverse=True)[0]


def rebuild_resolved_nest_parts(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM resolved_nest_parts")
    resolution_cache = _build_resolution_cache(connection)

    nest_rows = connection.execute(
        """
        SELECT
            pn.id AS nest_id,
            pn.barcode_filename,
            pn.program_date,
            pn.build_date_code,
            pn.order_number_raw,
            np.id AS nest_part_id,
            np.part_number,
            np.part_revision,
            np.quantity_nested
        FROM program_nests pn
        JOIN nest_parts np ON np.nest_id = pn.id
        ORDER BY pn.id, np.part_number, np.id
        """
    ).fetchall()

    grouped: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in nest_rows:
        grouped[int(row["nest_id"])].append(row)

    for nest_id, part_rows in grouped.items():
        selected_job_id, job_candidates = _collect_job_candidates(connection, part_rows, resolution_cache)
        selected_job_row = None
        selected_job_score = 0
        if selected_job_id is not None:
            selected_job_row = resolution_cache["job_folders_by_id"].get(selected_job_id)
            if selected_job_row is None:
                selected_job_row = connection.execute(
                    "SELECT * FROM job_folders WHERE id = ?",
                    (selected_job_id,),
                ).fetchone()
            selected_job_score = int(job_candidates[selected_job_id]["score"])

        candidate_rows_for_metadata = [info["job_row"] for info in job_candidates.values()] if job_candidates else []
        base_candidate_count, base_build_dates, base_com_numbers = _candidate_metadata_from_rows(candidate_rows_for_metadata)

        for row in part_rows:
            result = _resolve_with_part_attributes(connection, row, resolution_cache)
            if selected_job_row is not None:
                assert selected_job_id is not None
                matched_job_part = _select_best_job_part(connection, row, selected_job_id, resolution_cache)
                matched_label = _select_best_label(connection, row, selected_job_id, resolution_cache)
                if matched_job_part or matched_label:
                    attribute_match = _select_attribute_for_job(connection, row, selected_job_row, resolution_cache)
                    evidence = {
                        "job_folder": selected_job_row["folder_name"],
                        "job_com_number": selected_job_row["com_number"],
                        "job_build_date": selected_job_row["build_date_code"],
                        "job_part_source": matched_job_part["source_type"] if matched_job_part else None,
                        "label_nest_name": matched_label["nest_name"] if matched_label else None,
                    }
                    result = {
                        "com_number": selected_job_row["com_number"],
                        "form_value": attribute_match["form"] if attribute_match else None,
                        "requires_forming": attribute_match["requires_forming"] if attribute_match else None,
                        "nested_on": matched_job_part["nested_on"] if matched_job_part else (attribute_match["nested_on"] if attribute_match else None),
                        "resolution_status": "resolved" if matched_job_part else "partial",
                        "resolution_rule": "job_context" if matched_job_part else "job_context_label_only",
                        "match_candidate_count": base_candidate_count,
                        "match_build_dates": base_build_dates,
                        "match_com_numbers": base_com_numbers,
                        "matched_part_attribute_id": attribute_match["id"] if attribute_match else None,
                        "matched_job_folder_id": selected_job_id,
                        "matched_job_part_id": matched_job_part["id"] if matched_job_part else None,
                        "job_match_score": selected_job_score,
                        "evidence_summary": json.dumps(evidence),
                    }

            connection.execute(
                """
                INSERT INTO resolved_nest_parts (
                    nest_id, nest_part_id, barcode_filename, build_date_code, order_number_raw,
                    part_number, part_revision, quantity_nested, com_number, form_value,
                    requires_forming, nested_on, resolution_status, resolution_rule,
                    match_candidate_count, match_build_dates, match_com_numbers,
                    matched_job_folder_id, matched_job_part_id, job_match_score, evidence_summary,
                    matched_part_attribute_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    nest_id,
                    row["nest_part_id"],
                    row["barcode_filename"],
                    row["build_date_code"],
                    row["order_number_raw"],
                    row["part_number"],
                    row["part_revision"],
                    row["quantity_nested"],
                    result["com_number"],
                    result["form_value"],
                    result["requires_forming"],
                    result["nested_on"],
                    result["resolution_status"],
                    result["resolution_rule"],
                    result["match_candidate_count"],
                    result["match_build_dates"],
                    result["match_com_numbers"],
                    result["matched_job_folder_id"],
                    result["matched_job_part_id"],
                    result["job_match_score"],
                    result["evidence_summary"],
                    result["matched_part_attribute_id"],
                ),
            )


def resolve_nest_parts_for_ids(connection: sqlite3.Connection, nest_ids: list[int]) -> None:
    unique_nest_ids = sorted({int(nest_id) for nest_id in nest_ids if int(nest_id) > 0})
    if not unique_nest_ids:
        return

    placeholders = ",".join("?" for _ in unique_nest_ids)
    connection.execute(
        f"DELETE FROM resolved_nest_parts WHERE nest_id IN ({placeholders})",
        unique_nest_ids,
    )
    resolution_cache = _build_resolution_cache(connection)
    nest_rows = connection.execute(
        f"""
        SELECT
            pn.id AS nest_id,
            pn.barcode_filename,
            pn.program_date,
            pn.build_date_code,
            pn.order_number_raw,
            np.id AS nest_part_id,
            np.part_number,
            np.part_revision,
            np.quantity_nested
        FROM program_nests pn
        JOIN nest_parts np ON np.nest_id = pn.id
        WHERE pn.id IN ({placeholders})
        ORDER BY pn.id, np.part_number, np.id
        """,
        unique_nest_ids,
    ).fetchall()

    grouped: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in nest_rows:
        grouped[int(row["nest_id"])] .append(row)

    for nest_id, part_rows in grouped.items():
        selected_job_id, job_candidates = _collect_job_candidates(connection, part_rows, resolution_cache)
        selected_job_row = None
        selected_job_score = 0
        if selected_job_id is not None:
            selected_job_row = resolution_cache["job_folders_by_id"].get(selected_job_id)
            if selected_job_row is None:
                selected_job_row = connection.execute(
                    "SELECT * FROM job_folders WHERE id = ?",
                    (selected_job_id,),
                ).fetchone()
            selected_job_score = int(job_candidates[selected_job_id]["score"])

        candidate_rows_for_metadata = [info["job_row"] for info in job_candidates.values()] if job_candidates else []
        base_candidate_count, base_build_dates, base_com_numbers = _candidate_metadata_from_rows(candidate_rows_for_metadata)

        for row in part_rows:
            result = _resolve_with_part_attributes(connection, row, resolution_cache)
            if selected_job_row is not None:
                assert selected_job_id is not None
                matched_job_part = _select_best_job_part(connection, row, selected_job_id, resolution_cache)
                matched_label = _select_best_label(connection, row, selected_job_id, resolution_cache)
                if matched_job_part or matched_label:
                    attribute_match = _select_attribute_for_job(connection, row, selected_job_row, resolution_cache)
                    evidence = {
                        "job_folder": selected_job_row["folder_name"],
                        "job_com_number": selected_job_row["com_number"],
                        "job_build_date": selected_job_row["build_date_code"],
                        "job_part_source": matched_job_part["source_type"] if matched_job_part else None,
                        "label_nest_name": matched_label["nest_name"] if matched_label else None,
                    }
                    result = {
                        "com_number": selected_job_row["com_number"],
                        "form_value": attribute_match["form"] if attribute_match else None,
                        "requires_forming": attribute_match["requires_forming"] if attribute_match else None,
                        "nested_on": matched_job_part["nested_on"] if matched_job_part else (attribute_match["nested_on"] if attribute_match else None),
                        "resolution_status": "resolved" if matched_job_part else "partial",
                        "resolution_rule": "job_context" if matched_job_part else "job_context_label_only",
                        "match_candidate_count": base_candidate_count,
                        "match_build_dates": base_build_dates,
                        "match_com_numbers": base_com_numbers,
                        "matched_part_attribute_id": attribute_match["id"] if attribute_match else None,
                        "matched_job_folder_id": selected_job_id,
                        "matched_job_part_id": matched_job_part["id"] if matched_job_part else None,
                        "job_match_score": selected_job_score,
                        "evidence_summary": json.dumps(evidence),
                    }

            connection.execute(
                """
                INSERT INTO resolved_nest_parts (
                    nest_id, nest_part_id, barcode_filename, build_date_code, order_number_raw,
                    part_number, part_revision, quantity_nested, com_number, form_value,
                    requires_forming, nested_on, resolution_status, resolution_rule,
                    match_candidate_count, match_build_dates, match_com_numbers,
                    matched_job_folder_id, matched_job_part_id, job_match_score, evidence_summary,
                    matched_part_attribute_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    nest_id,
                    row["nest_part_id"],
                    row["barcode_filename"],
                    row["build_date_code"],
                    row["order_number_raw"],
                    row["part_number"],
                    row["part_revision"],
                    row["quantity_nested"],
                    result["com_number"],
                    result["form_value"],
                    result["requires_forming"],
                    result["nested_on"],
                    result["resolution_status"],
                    result["resolution_rule"],
                    result["match_candidate_count"],
                    result["match_build_dates"],
                    result["match_com_numbers"],
                    result["matched_job_folder_id"],
                    result["matched_job_part_id"],
                    result["job_match_score"],
                    result["evidence_summary"],
                    result["matched_part_attribute_id"],
                ),
            )


def _scan_supported_files(
    roots: list[Path],
    *,
    changed_since: datetime | None,
    processed_paths: set[str],
    progress_callback: ProgressCallback | None,
    warning_callback: WarningCallback | None,
) -> tuple[list[tuple[Path, str]], int, int]:
    cache = load_scan_cache()
    cached_roots = {
        str(root_path).casefold(): root_node
        for root_path, root_node in cache.get("roots", {}).items()
        if isinstance(root_node, Mapping)
    }
    deferred_paths = {str(path).casefold() for path in cache.get("deferred_supported_files", [])}
    next_deferred: set[str] = set()
    discovered: list[tuple[Path, str]] = []
    seen: set[str] = set()
    unstable_recent_files = 0
    filtered_old_files = 0
    now = datetime.now()
    updated_roots: dict[str, Any] = {}

    for index, root in enumerate(roots, start=1):
        if not root.exists() or not root.is_dir():
            updated_roots[str(root)] = {
                "path": str(root),
                "recursive": should_scan_recursively(root),
                "mtime_ns": 0,
                "all_supported_files": [],
                "filtered_old_files": 0,
                "children": {},
            }
            continue

        root_files, root_filtered_old, root_node, root_unstable = _scan_directory_tree(
            root,
            recursive=should_scan_recursively(root),
            now=now,
            changed_since=changed_since,
            processed_paths=processed_paths,
            deferred_paths=deferred_paths,
            next_deferred=next_deferred,
            cache_node=cached_roots.get(str(root).casefold()),
            root_index=index,
            total_roots=len(roots),
            is_root=True,
        )
        updated_roots[str(root)] = root_node
        filtered_old_files += root_filtered_old
        unstable_recent_files += root_unstable

        save_scan_cache(
            {"roots": updated_roots, "deferred_supported_files": sorted(next_deferred)},
            warning_callback=warning_callback,
        )

        for path in root_files:
            normalized_path = str(path).casefold()
            if normalized_path in seen:
                continue
            file_type = classify_file(path)
            if file_type is None:
                continue
            seen.add(normalized_path)
            discovered.append((path, file_type))

        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "Scanning folders",
                    "message": f"Scanning {root}",
                    "scanned_roots": index,
                    "total_roots": len(roots),
                    "discovered_supported_files": len(discovered),
                    "unstable_recent_files": unstable_recent_files,
                    "filtered_old_files": filtered_old_files,
                }
            )

    for deferred_path in cache.get("deferred_supported_files", []):
        path = Path(str(deferred_path))
        normalized_path = str(path).casefold()
        if normalized_path in seen or is_ignored_source_path(path):
            continue
        if is_immutable_source_path(path) and normalized_path in processed_paths:
            continue
        file_type = classify_file(path)
        if file_type is None:
            continue
        stat = _safe_stat(path)
        if stat is None:
            if is_immutable_source_path(path):
                raise SourceAccessError(f"Lost access to P drive while rechecking deferred file: {path}")
            continue
        recent_enough = is_recent_enough(path, now)
        if recent_enough is None or not recent_enough:
            continue
        if datetime.fromtimestamp(stat.st_mtime) > now - IMPORT_STABILITY_WINDOW:
            next_deferred.add(str(path))
            continue
        seen.add(normalized_path)
        discovered.append((path, file_type))

    save_scan_cache(
        {"roots": updated_roots, "deferred_supported_files": sorted(next_deferred)},
        warning_callback=warning_callback,
    )
    return sorted(discovered, key=lambda item: str(item[0]).casefold()), unstable_recent_files, filtered_old_files


def import_paths(
    roots: list[Path],
    *,
    changed_since: datetime | None = None,
    progress_callback: ProgressCallback | None = None,
    warning_callback: WarningCallback | None = None,
) -> dict[str, int]:
    with get_connection() as connection:
        create_schema(connection)
        processed_paths = load_processed_paths(connection)
        discovered, unstable_recent_files, filtered_old_files = _scan_supported_files(
            roots,
            changed_since=changed_since,
            processed_paths=processed_paths,
            progress_callback=progress_callback,
            warning_callback=warning_callback,
        )
        imported_nest_ids: list[int] = []

        counts = {
            "processed": 0,
            "skipped": 0,
            "unchanged_skipped": 0,
            "duplicate_candidate_skipped": 0,
            "missing_skipped": 0,
            "errors": 0,
            "missing_files": 0,
            "total_supported_files": len(discovered),
            "nest_files": 0,
            "dat_files": 0,
            "dat_groups": 0,
            "duplicate_dat_files": 0,
            "filtered_old_files": filtered_old_files,
            "unstable_recent_files": unstable_recent_files,
            "total_steps": 0,
        }

        by_type: dict[str, list[Path]] = defaultdict(list)
        for path, file_type in discovered:
            by_type[file_type].append(path)

        counts["nest_files"] = len(by_type.get("nest_comparison", []))
        counts["dat_files"] = len(by_type.get("amada_dat", []))

        grouped_dat_files: dict[str, list[Path]] = defaultdict(list)
        for path in by_type.get("amada_dat", []):
            grouped_dat_files[path.name.upper()].append(path)
        counts["dat_groups"] = len(grouped_dat_files)
        counts["duplicate_dat_files"] = sum(max(0, len(candidates) - 1) for candidates in grouped_dat_files.values())

        ordered_non_dat: list[Path] = []
        for file_type in ("nest_comparison", "yanoprog", "spp_label_file", "order_in", "channel_rollformer"):
            ordered_non_dat.extend(sorted(by_type.get(file_type, []), key=lambda item: str(item).casefold()))

        counts["total_steps"] = len(ordered_non_dat) + len(grouped_dat_files) + 1
        current_step = 0

        def emit(phase: str, message: str, current_file: str = "") -> None:
            if progress_callback is None:
                return
            progress_callback(
                {
                    "phase": phase,
                    "message": message,
                    "current_file": current_file,
                    "current_step": current_step,
                    "total_steps": counts["total_steps"],
                    "processed": counts["processed"],
                    "skipped": counts["skipped"],
                    "errors": counts["errors"],
                    "missing_files": counts["missing_files"],
                    "total_supported_files": counts["total_supported_files"],
                    "nest_files": counts["nest_files"],
                    "dat_files": counts["dat_files"],
                    "dat_groups": counts["dat_groups"],
                    "duplicate_dat_files": counts["duplicate_dat_files"],
                    "filtered_old_files": counts["filtered_old_files"],
                    "unstable_recent_files": counts["unstable_recent_files"],
                    "scanned_roots": len(roots),
                    "total_roots": len(roots),
                    "discovered_supported_files": counts["total_supported_files"],
                }
            )

        for path in ordered_non_dat:
            emit("Importing job files", f"Checking {path.name}", str(path))
            try:
                if hasattr(connection, "ensure_connected"):
                    connection.ensure_connected()
                if not should_process_file(connection, path):
                    counts["skipped"] += 1
                    counts["unchanged_skipped"] += 1
                    current_step += 1
                    continue
                import_file(connection, path, roots)
                connection.commit()
                counts["processed"] += 1
            except FileNotFoundError:
                counts["skipped"] += 1
                counts["missing_skipped"] += 1
                counts["missing_files"] += 1
            current_step += 1

        for barcode_filename, candidates in sorted(grouped_dat_files.items()):
            current_source = _current_program_source(connection, barcode_filename)
            if current_source is not None:
                emit("Importing DAT files", f"Skipping {barcode_filename} (already imported)", current_source)
                counts["skipped"] += 1
                counts["unchanged_skipped"] += 1
                for candidate in candidates[1:]:
                    counts["skipped"] += 1
                    counts["duplicate_candidate_skipped"] += 1
                current_step += 1
                continue

            try:
                selected_path, _reason = _select_best_dat_candidate(connection, candidates)
            except FileNotFoundError:
                counts["skipped"] += len(candidates)
                counts["missing_skipped"] += len(candidates)
                counts["missing_files"] += len(candidates)
                current_step += 1
                continue
            emit("Importing DAT files", f"Importing {selected_path.name}", str(selected_path))
            try:
                if hasattr(connection, "ensure_connected"):
                    connection.ensure_connected()
                needs_import = should_process_file(connection, selected_path)

                if needs_import:
                    imported_nest_id = import_file(connection, selected_path, roots)
                    if imported_nest_id is not None:
                        imported_nest_ids.append(imported_nest_id)
                    connection.commit()
                    counts["processed"] += 1
                else:
                    counts["skipped"] += 1
                    counts["unchanged_skipped"] += 1
            except FileNotFoundError:
                counts["skipped"] += 1
                counts["missing_skipped"] += 1
                counts["missing_files"] += 1

            for candidate in candidates:
                if candidate != selected_path:
                    counts["skipped"] += 1
                    counts["duplicate_candidate_skipped"] += 1
            current_step += 1

        if imported_nest_ids:
            emit("Resolving parts", f"Resolving {len(imported_nest_ids)} newly imported nests")
            resolve_nest_parts_for_ids(connection, imported_nest_ids)
        else:
            emit("Resolving parts", "No new DAT files imported; skipping resolved part rebuild")
        current_step += 1

        connection.commit()
        return counts


def correction_import_paths(
    roots: list[Path],
    *,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, int]:
    return import_paths(roots, changed_since=None, progress_callback=progress_callback)


def import_test_data(root: Path) -> dict[str, int]:
    return import_paths([root])
