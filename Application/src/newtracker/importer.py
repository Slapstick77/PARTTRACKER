from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from .db import DATA_DIR, get_connection
from .parser import ParsedNestPart
from .parser import file_sha256, parse_dat_file, parse_nest_comparison_csv
from .schema import create_schema

SUPPORTED_PATTERNS = {
    ".dat": "amada_dat",
    "nestcomparison.csv": "nest_comparison",
}

# Bump when DAT parsing behavior changes and existing unchanged DAT files
# should be reprocessed to refresh persisted nest_parts rows.
AMADA_DAT_PARSER_VERSION = "2026-03-27-laser-npt-v2"
MAX_IMPORT_FILE_AGE = timedelta(days=183)
SCAN_CACHE_PATH = DATA_DIR / "import_scan_cache.json"
SCAN_CACHE_VERSION = 2

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

ImportProgressCallback = Callable[[dict[str, Any]], None]


def _empty_scan_cache() -> dict[str, Any]:
    return {"version": SCAN_CACHE_VERSION, "roots": {}}


def load_scan_cache() -> dict[str, Any]:
    if not SCAN_CACHE_PATH.exists():
        return _empty_scan_cache()

    try:
        payload = json.loads(SCAN_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_scan_cache()

    if not isinstance(payload, dict) or payload.get("version") != SCAN_CACHE_VERSION:
        return _empty_scan_cache()

    roots = payload.get("roots")
    if not isinstance(roots, dict):
        return _empty_scan_cache()

    return {"version": SCAN_CACHE_VERSION, "roots": roots}


def save_scan_cache(cache: Mapping[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": SCAN_CACHE_VERSION,
        "roots": dict(cache.get("roots", {})) if isinstance(cache, Mapping) else {},
    }
    SCAN_CACHE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def clear_scan_cache() -> None:
    try:
        SCAN_CACHE_PATH.unlink()
    except FileNotFoundError:
        pass


def _directory_mtime_ns(path: Path) -> int:
    stat = path.stat()
    return getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))


def _safe_stat(path: Path):
    try:
        return path.stat()
    except OSError:
        return None


def is_ignored_source_path(path: Path) -> bool:
    candidate = str(path).casefold()
    for ignored in IGNORED_SOURCE_PATH_KEYS:
        if candidate == ignored or candidate.startswith(f"{ignored}\\"):
            return True
    return False


def _normalize_cached_node(node: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(node, Mapping):
        return None

    recursive = bool(node.get("recursive"))
    cached_files = node.get("all_supported_files")
    children = node.get("children")
    return {
        "path": str(node.get("path", "")),
        "recursive": recursive,
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
    cache_node: Mapping[str, Any] | None,
    progress_callback: ImportProgressCallback | None = None,
    progress_state: dict[str, Any] | None = None,
    root_index: int = 0,
    total_roots: int = 0,
    is_root: bool = False,
) -> tuple[list[Path], int, dict[str, Any]]:
    if progress_state is None:
        progress_state = {"discovered_supported_files": 0, "filtered_old_files": 0}

    if is_ignored_source_path(path):
        return [], 0, {
            "path": str(path),
            "recursive": recursive,
            "mtime_ns": 0,
            "all_supported_files": [],
            "filtered_old_files": 0,
            "children": {},
        }

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
        }

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
            }

    if normalized_cache and normalized_cache["recursive"] == recursive and normalized_cache["mtime_ns"] == mtime_ns:
        cached_paths = [Path(value) for value in normalized_cache["all_supported_files"]]
        progress_state["discovered_supported_files"] = int(progress_state.get("discovered_supported_files", 0)) + len(cached_paths)
        progress_state["filtered_old_files"] = int(progress_state.get("filtered_old_files", 0)) + int(
            normalized_cache["filtered_old_files"]
        )
        if progress_callback and progress_state["discovered_supported_files"] and progress_state["discovered_supported_files"] % 250 == 0:
            _emit_progress(
                progress_callback,
                event="scan_progress",
                phase="Scanning folders",
                message=f"Found {progress_state['discovered_supported_files']} supported files so far.",
                current_file=str(path),
                scanned_roots=max(0, root_index - 1),
                total_roots=total_roots,
                discovered_supported_files=progress_state["discovered_supported_files"],
                filtered_old_files=progress_state["filtered_old_files"],
            )
        return cached_paths, normalized_cache["filtered_old_files"], normalized_cache

    supported_files: list[Path] = []
    filtered_old_files = 0
    children: dict[str, Any] = {}
    child_cache_map = {}
    if normalized_cache:
        child_cache_map = {
            str(child_path).casefold(): child_node
            for child_path, child_node in normalized_cache.get("children", {}).items()
        }

    try:
        entries = sorted(path.iterdir(), key=lambda entry: entry.name.casefold())
    except OSError:
        entries = []

    for entry in entries:
        if entry.is_dir():
            if is_ignored_source_path(entry):
                continue
            if not recursive:
                continue
            child_files, child_filtered_old, child_node = _scan_directory_tree(
                entry,
                recursive=True,
                now=now,
                changed_since=changed_since,
                cache_node=child_cache_map.get(str(entry).casefold()),
                progress_callback=progress_callback,
                progress_state=progress_state,
                root_index=root_index,
                total_roots=total_roots,
                is_root=False,
            )
            supported_files.extend(child_files)
            filtered_old_files += child_filtered_old
            children[str(entry)] = child_node
            continue

        if not entry.is_file():
            continue

        if is_ignored_source_path(entry):
            continue

        if not classify_file(entry):
            continue

        if changed_since is not None:
            entry_stat = _safe_stat(entry)
            if entry_stat is None:
                continue
            if datetime.fromtimestamp(entry_stat.st_mtime) < changed_since:
                continue

        recent_enough = is_recent_enough(entry, now=now)
        if recent_enough is None:
            continue

        if not recent_enough:
            filtered_old_files += 1
            progress_state["filtered_old_files"] = int(progress_state.get("filtered_old_files", 0)) + 1
            if progress_callback and progress_state["filtered_old_files"] % 250 == 0:
                _emit_progress(
                    progress_callback,
                    event="scan_age_filtered",
                    phase="Scanning folders",
                    message=f"Skipped {progress_state['filtered_old_files']} supported files older than 6 months.",
                    current_file=str(entry),
                    scanned_roots=max(0, root_index - 1),
                    total_roots=total_roots,
                    discovered_supported_files=int(progress_state.get("discovered_supported_files", 0)),
                    filtered_old_files=progress_state["filtered_old_files"],
                )
            continue

        supported_files.append(entry)
        progress_state["discovered_supported_files"] = int(progress_state.get("discovered_supported_files", 0)) + 1
        if progress_callback and progress_state["discovered_supported_files"] % 250 == 0:
            _emit_progress(
                progress_callback,
                event="scan_progress",
                phase="Scanning folders",
                message=f"Found {progress_state['discovered_supported_files']} supported files so far.",
                current_file=str(entry),
                scanned_roots=max(0, root_index - 1),
                total_roots=total_roots,
                discovered_supported_files=progress_state["discovered_supported_files"],
                filtered_old_files=int(progress_state.get("filtered_old_files", 0)),
            )

    node = {
        "path": str(path),
        "recursive": recursive,
        "mtime_ns": mtime_ns,
        "all_supported_files": [str(file_path) for file_path in supported_files],
        "filtered_old_files": filtered_old_files,
        "children": children,
    }
    return supported_files, filtered_old_files, node


def canonical_key(path: Path) -> str:
    return path.name.lower() if path.name.lower() == "nestcomparison.csv" else path.suffix.lower()


def classify_file(path: Path) -> str | None:
    lower_name = path.name.lower()
    if lower_name == "nestcomparison.csv":
        return "nest_comparison"
    if path.suffix.lower() == ".dat":
        return "amada_dat"
    return None


def is_recent_enough(path: Path, now: datetime | None = None) -> bool | None:
    stat = _safe_stat(path)
    if stat is None:
        return None
    created_at = datetime.fromtimestamp(stat.st_ctime)
    cutoff = (now or datetime.now()) - MAX_IMPORT_FILE_AGE
    return created_at >= cutoff


def should_scan_recursively(root: Path) -> bool:
    return "programming" in root.name.casefold()


def iter_supported_candidates(root: Path) -> list[Path]:
    globber = root.rglob if should_scan_recursively(root) else root.glob
    candidates = list(globber("*.DAT"))
    candidates.extend(globber("NestComparison.csv"))
    candidates.extend(globber("nestcomparison.csv"))
    return candidates


def file_content_fingerprint(path: Path, file_type: str | None = None) -> str:
    resolved_type = file_type or classify_file(path) or "unknown"
    try:
        base_hash = file_sha256(path)
    except OSError as exc:
        raise FileNotFoundError(f"File is not readable: {path}") from exc
    if resolved_type == "amada_dat":
        return f"{base_hash}|parser={AMADA_DAT_PARSER_VERSION}"
    return base_hash


def path_is_available(path: Path) -> bool:
    try:
        return path.exists() and path.is_file()
    except OSError:
        return False


def _count_part_attribute_matches(connection: sqlite3.Connection, part_numbers: list[str]) -> int:
    normalized = sorted({(part or "").strip() for part in part_numbers if (part or "").strip()})
    if not normalized:
        return 0
    placeholders = ",".join("?" for _ in normalized)
    row = connection.execute(
        f"""
        SELECT COUNT(DISTINCT part_number) AS cnt
        FROM part_attributes
        WHERE part_number IN ({placeholders})
        """,
        normalized,
    ).fetchone()
    return int(row["cnt"]) if row and row["cnt"] is not None else 0


def _current_program_source(connection: sqlite3.Connection, barcode_filename: str) -> str | None:
    row = connection.execute(
        """
        SELECT source_file_path
        FROM program_nests
        WHERE UPPER(barcode_filename) = UPPER(?)
        """,
        (barcode_filename,),
    ).fetchone()
    return row["source_file_path"] if row else None


def _same_path(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.casefold() == b.casefold()


def _select_best_dat_candidate(connection: sqlite3.Connection, candidates: list[Path]) -> tuple[Path, str]:
    """Pick the best DAT source for a shared barcode filename.

    Scoring (higher is better):
    1) matched part count against part_attributes
    2) total nested quantity
    3) unique part count
    4) parsed part row count
    5) deterministic path tie-break (lexicographically descending)
    """
    available_candidates = [path for path in candidates if path_is_available(path)]
    if not available_candidates:
        raise FileNotFoundError("No available DAT candidates remained for this barcode filename.")

    if len(available_candidates) == 1:
        return available_candidates[0], "single_candidate"

    scored: list[tuple[tuple[int, int, int, int, str], Path]] = []
    for path in available_candidates:
        try:
            parsed = parse_dat_file(path)
        except OSError:
            continue
        quantities = [max(0, int(part.quantity_nested)) for part in parsed.parts]
        total_quantity = sum(quantities)
        unique_parts = len({(part.part_number or "").strip() for part in parsed.parts if (part.part_number or "").strip()})
        match_count = _count_part_attribute_matches(connection, [part.part_number for part in parsed.parts])
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


def should_process_file(connection: sqlite3.Connection, path: Path) -> bool:
    if not path_is_available(path):
        return False
    stat = _safe_stat(path)
    if stat is None:
        raise FileNotFoundError(f"File is no longer available: {path}")
    file_type = classify_file(path)
    row = connection.execute(
        "SELECT file_size, modified_time, content_hash FROM processed_files WHERE file_path = ?",
        (str(path),),
    ).fetchone()
    if row is None:
        return True

    expected_fingerprint = file_content_fingerprint(path, file_type)
    if row["file_size"] != stat.st_size or float(row["modified_time"]) != stat.st_mtime:
        if (row["content_hash"] or "") == expected_fingerprint:
            refresh_processed_file_metadata(connection, path=path, file_type=file_type or "unknown", content_hash=expected_fingerprint)
            return False
        return True

    return (row["content_hash"] or "") != expected_fingerprint


def refresh_processed_file_metadata(
    connection: sqlite3.Connection,
    *,
    path: Path,
    file_type: str,
    content_hash: str | None,
) -> None:
    stat = _safe_stat(path)
    if stat is None:
        return
    connection.execute(
        """
        UPDATE processed_files
        SET file_name = ?,
            file_type = ?,
            file_size = ?,
            modified_time = ?,
            content_hash = ?
        WHERE file_path = ?
        """,
        (path.name, file_type, stat.st_size, stat.st_mtime, content_hash, str(path)),
    )


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


def _normalize_date_token(value: str | None) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    return digits


def _program_date_prefix(value: str | None) -> str:
    token = _normalize_date_token(value)
    return token[:8] if len(token) >= 8 else ""


def _candidate_metadata(candidates: list[sqlite3.Row]) -> tuple[int, str | None, str | None]:
    if not candidates:
        return 0, None, None

    build_dates = []
    com_numbers = []
    for candidate in candidates:
        build_date = candidate["build_date"]
        com_number = candidate["com_number"]
        if build_date and build_date not in build_dates:
            build_dates.append(build_date)
        if com_number and com_number not in com_numbers:
            com_numbers.append(com_number)

    return len(candidates), json.dumps(build_dates) if build_dates else None, json.dumps(com_numbers) if com_numbers else None


def _coalesce_text_values(values: list[str]) -> str:
    unique_values: list[str] = []
    for value in values:
        cleaned = (value or "").strip()
        if cleaned and cleaned not in unique_values:
            unique_values.append(cleaned)
    return " | ".join(unique_values)


def aggregate_nest_parts(parts: list[ParsedNestPart]) -> list[ParsedNestPart]:
    grouped: dict[tuple[str, str], list[ParsedNestPart]] = defaultdict(list)
    order: list[tuple[str, str]] = []

    for part in parts:
        key = (part.part_number, part.part_revision or "")
        if key not in grouped:
            order.append(key)
        grouped[key].append(part)

    merged_parts: list[ParsedNestPart] = []
    for key in order:
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


def _fetch_nest_rows(
    connection: sqlite3.Connection,
    barcode_filenames: Iterable[str] | None = None,
) -> list[sqlite3.Row]:
    query = """
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
    """
    params: list[str] = []
    normalized_barcodes = sorted({(value or "").strip() for value in (barcode_filenames or []) if (value or "").strip()})
    if normalized_barcodes:
        placeholders = ",".join("?" for _ in normalized_barcodes)
        query += f" WHERE UPPER(pn.barcode_filename) IN ({placeholders})"
        params.extend(value.upper() for value in normalized_barcodes)
    query += " ORDER BY pn.barcode_filename, np.part_number"
    return connection.execute(query, params).fetchall()


def _insert_resolved_nest_part_row(connection: sqlite3.Connection, row: sqlite3.Row) -> None:
    normalized_rev = normalize_revision(row["part_revision"])
    build_date = row["build_date_code"] or ""
    program_date_prefix = _program_date_prefix(row["program_date"])

    candidates = connection.execute(
        """
        SELECT id, com_number, form, requires_forming, nested_on, build_date
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
        candidates = connection.execute(
            """
            SELECT id, com_number, form, requires_forming, nested_on, build_date
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
            candidates = connection.execute(
                """
                SELECT id, com_number, form, requires_forming, nested_on, build_date
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

    match_candidate_count, match_build_dates, match_com_numbers = _candidate_metadata(selected_candidates)

    connection.execute(
        """
        INSERT INTO resolved_nest_parts (
            nest_id, nest_part_id, barcode_filename, build_date_code, order_number_raw,
            part_number, part_revision, quantity_nested, com_number, form_value,
            requires_forming, nested_on, resolution_status, resolution_rule,
            match_candidate_count, match_build_dates, match_com_numbers, matched_part_attribute_id,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            row["nest_id"],
            row["nest_part_id"],
            row["barcode_filename"],
            row["build_date_code"],
            row["order_number_raw"],
            row["part_number"],
            row["part_revision"],
            row["quantity_nested"],
            matched["com_number"] if matched else None,
            matched["form"] if matched else None,
            matched["requires_forming"] if matched else None,
            matched["nested_on"] if matched else None,
            resolution_status,
            resolution_rule,
            match_candidate_count,
            match_build_dates,
            match_com_numbers,
            matched["id"] if matched else None,
        ),
    )


def rebuild_resolved_nest_parts(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM resolved_nest_parts")

    nest_rows = _fetch_nest_rows(connection)

    for row in nest_rows:
        _insert_resolved_nest_part_row(connection, row)


def rebuild_resolved_nest_parts_for_barcodes(
    connection: sqlite3.Connection,
    barcode_filenames: Iterable[str],
) -> int:
    normalized_barcodes = sorted({(value or "").strip() for value in barcode_filenames if (value or "").strip()})
    if not normalized_barcodes:
        return 0

    placeholders = ",".join("?" for _ in normalized_barcodes)
    connection.execute(
        f"DELETE FROM resolved_nest_parts WHERE UPPER(barcode_filename) IN ({placeholders})",
        [value.upper() for value in normalized_barcodes],
    )

    nest_rows = _fetch_nest_rows(connection, normalized_barcodes)
    for row in nest_rows:
        _insert_resolved_nest_part_row(connection, row)

    return len(nest_rows)


def import_dat_file(connection: sqlite3.Connection, path: Path) -> None:
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
    nest_id = cursor.lastrowid

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
                part.part_revision or "",
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


def import_nest_comparison(connection: sqlite3.Connection, path: Path) -> None:
    connection.execute("DELETE FROM part_attributes WHERE source_file_path = ?", (str(path),))
    for row in parse_nest_comparison_csv(path):
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
            (
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
            ),
        )


def scan_supported_files(
    roots: Iterable[Path],
    progress_callback: ImportProgressCallback | None = None,
    *,
    changed_since: datetime | None = None,
) -> tuple[list[Path], int]:
    files: list[Path] = []
    seen: set[str] = set()
    filtered_old_files = 0
    root_list = list(roots)
    now = datetime.now()
    scan_cache = load_scan_cache()
    cached_roots = {
        str(root_path).casefold(): root_node
        for root_path, root_node in scan_cache.get("roots", {}).items()
        if isinstance(root_node, Mapping)
    }
    updated_roots: dict[str, Any] = {}
    progress_state = {"discovered_supported_files": 0, "filtered_old_files": 0}
    for root_index, root in enumerate(root_list, start=1):
        _emit_progress(
            progress_callback,
            event="scan_root_started",
            phase="Scanning folders",
            message=f"Scanning folder {root_index} of {len(root_list)}: {root}",
            current_file=str(root),
            scanned_roots=root_index - 1,
            total_roots=len(root_list),
            discovered_supported_files=len(files),
        )
        if not root.exists() or not root.is_dir():
            _emit_progress(
                progress_callback,
                event="scan_root_missing",
                phase="Scanning folders",
                message=f"Folder not available: {root}",
                current_file=str(root),
                scanned_roots=root_index,
                total_roots=len(root_list),
                discovered_supported_files=len(files),
            )
            continue
        discovered_before = len(files)
        root_supported_files, root_filtered_old_files, root_node = _scan_directory_tree(
            root,
            recursive=should_scan_recursively(root),
            now=now,
            changed_since=changed_since,
            cache_node=cached_roots.get(str(root).casefold()),
            progress_callback=progress_callback,
            progress_state=progress_state,
            root_index=root_index,
            total_roots=len(root_list),
            is_root=True,
        )
        updated_roots[str(root)] = root_node
        save_scan_cache({"version": SCAN_CACHE_VERSION, "roots": updated_roots})
        for path in root_supported_files:
            normalized = str(path).casefold()
            if normalized in seen:
                continue
            seen.add(normalized)
            files.append(path)

        prior_filtered_old_files = filtered_old_files
        filtered_old_files += root_filtered_old_files
        if filtered_old_files and filtered_old_files // 250 > prior_filtered_old_files // 250:
            _emit_progress(
                progress_callback,
                event="scan_age_filtered",
                phase="Scanning folders",
                message=f"Skipped {filtered_old_files} supported files older than 6 months.",
                current_file=str(root),
                scanned_roots=root_index - 1,
                total_roots=len(root_list),
                discovered_supported_files=len(files),
                filtered_old_files=filtered_old_files,
            )
        _emit_progress(
            progress_callback,
            event="scan_root_completed",
            phase="Scanning folders",
            message=f"Completed scan of {root}. Found {len(files) - discovered_before} supported files in this folder.",
            current_file=str(root),
            scanned_roots=root_index,
            total_roots=len(root_list),
            discovered_supported_files=len(files),
            filtered_old_files=filtered_old_files,
        )
    save_scan_cache({"version": SCAN_CACHE_VERSION, "roots": updated_roots})
    return sorted(files), filtered_old_files


def import_file(connection: sqlite3.Connection, path: Path) -> None:
    file_type = classify_file(path)
    if file_type is None:
        return

    if not path_is_available(path):
        raise FileNotFoundError(f"File is no longer available: {path}")

    content_hash = file_content_fingerprint(path, file_type)
    try:
        if file_type == "amada_dat":
            import_dat_file(connection, path)
        elif file_type == "nest_comparison":
            import_nest_comparison(connection, path)
        upsert_processed_file(connection, path=path, file_type=file_type, status="processed", content_hash=content_hash)
    except OSError as exc:
        raise FileNotFoundError(f"File is not readable: {path}") from exc
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


def _emit_progress(callback: ImportProgressCallback | None, **payload: Any) -> None:
    if callback is None:
        return
    callback(payload)


def _import_paths_internal(
    roots: Iterable[Path],
    progress_callback: ImportProgressCallback | None = None,
    *,
    correction_run: bool = False,
    changed_since: datetime | None = None,
) -> dict[str, Any]:
    root_list = list(roots)
    with get_connection() as connection:
        create_schema(connection)
        counts: dict[str, Any] = {
            "processed": 0,
            "skipped": 0,
            "errors": 0,
            "missing_files": 0,
            "filtered_old_files": 0,
            "total_supported_files": 0,
            "nest_files": 0,
            "dat_files": 0,
            "dat_groups": 0,
            "duplicate_dat_files": 0,
            "total_steps": 0,
        }
        changed_dat_barcodes: set[str] = set()
        nest_comparison_changed = False
        existing_dat_barcodes = {
            str(row["barcode_filename"]).strip().upper()
            for row in connection.execute("SELECT barcode_filename FROM program_nests WHERE barcode_filename IS NOT NULL")
            if str(row["barcode_filename"] or "").strip()
        }

        files, filtered_old_files = scan_supported_files(
            root_list,
            progress_callback=progress_callback,
            changed_since=changed_since,
        )
        counts["filtered_old_files"] = filtered_old_files
        nest_files = [path for path in files if classify_file(path) == "nest_comparison"]
        dat_files = [path for path in files if classify_file(path) == "amada_dat"]
        grouped_dat_files: dict[str, list[Path]] = defaultdict(list)
        for path in dat_files:
            grouped_dat_files[path.name.upper()].append(path)

        counts["total_supported_files"] = len(files)
        counts["nest_files"] = len(nest_files)
        counts["dat_files"] = len(dat_files)
        counts["dat_groups"] = len(grouped_dat_files)
        counts["duplicate_dat_files"] = max(0, len(dat_files) - len(grouped_dat_files))
        counts["total_steps"] = len(nest_files) + len(grouped_dat_files) + 1

        step_index = 0
        _emit_progress(
            progress_callback,
            event="scan_complete",
            phase="Scanning folders",
            message=f"Found {len(files)} supported files across {len(root_list)} active folders.",
            filtered_old_files=0,
            total_supported_files=counts["total_supported_files"],
            nest_files=counts["nest_files"],
            dat_files=counts["dat_files"],
            dat_groups=counts["dat_groups"],
            duplicate_dat_files=counts["duplicate_dat_files"],
            current_step=0,
            total_steps=counts["total_steps"],
            processed=counts["processed"],
            skipped=counts["skipped"],
            errors=counts["errors"],
        )

        # Import NestComparison first so DAT duplicate scoring can leverage part_attributes coverage.
        for path in nest_files:
            step_index += 1
            _emit_progress(
                progress_callback,
                event="processing_file",
                phase="Importing nest comparison",
                message=f"Checking {path.name}",
                current_file=str(path),
                current_step=step_index,
                total_steps=counts["total_steps"],
                processed=counts["processed"],
                skipped=counts["skipped"],
                errors=counts["errors"],
                filtered_old_files=counts["filtered_old_files"],
                total_supported_files=counts["total_supported_files"],
                nest_files=counts["nest_files"],
                dat_files=counts["dat_files"],
                dat_groups=counts["dat_groups"],
                duplicate_dat_files=counts["duplicate_dat_files"],
            )
            if not path_is_available(path):
                counts["skipped"] += 1
                counts["missing_files"] += 1
                _emit_progress(
                    progress_callback,
                    event="file_missing",
                    phase="Importing nest comparison",
                    message=f"Skipped missing file {path.name}",
                    current_file=str(path),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
                continue

            try:
                if not should_process_file(connection, path):
                    counts["skipped"] += 1
                    _emit_progress(
                        progress_callback,
                        event="file_skipped",
                        phase="Importing nest comparison",
                        message=f"Skipped unchanged file {path.name}",
                        current_file=str(path),
                        current_step=step_index,
                        total_steps=counts["total_steps"],
                        processed=counts["processed"],
                        skipped=counts["skipped"],
                        errors=counts["errors"],
                        missing_files=counts["missing_files"],
                        filtered_old_files=counts["filtered_old_files"],
                        total_supported_files=counts["total_supported_files"],
                        nest_files=counts["nest_files"],
                        dat_files=counts["dat_files"],
                        dat_groups=counts["dat_groups"],
                        duplicate_dat_files=counts["duplicate_dat_files"],
                    )
                    continue
                import_file(connection, path)
            except FileNotFoundError:
                counts["skipped"] += 1
                counts["missing_files"] += 1
                _emit_progress(
                    progress_callback,
                    event="file_missing",
                    phase="Importing nest comparison",
                    message=f"Skipped missing file {path.name}",
                    current_file=str(path),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
                continue
            counts["processed"] += 1
            nest_comparison_changed = True
            _emit_progress(
                progress_callback,
                event="file_processed",
                phase="Importing nest comparison",
                message=f"Imported {path.name}",
                current_file=str(path),
                current_step=step_index,
                total_steps=counts["total_steps"],
                processed=counts["processed"],
                skipped=counts["skipped"],
                errors=counts["errors"],
                missing_files=counts["missing_files"],
                filtered_old_files=counts["filtered_old_files"],
                total_supported_files=counts["total_supported_files"],
                nest_files=counts["nest_files"],
                dat_files=counts["dat_files"],
                dat_groups=counts["dat_groups"],
                duplicate_dat_files=counts["duplicate_dat_files"],
            )

        for _, candidates in sorted(grouped_dat_files.items()):
            step_index += 1
            barcode_filename = candidates[0].name.upper() if candidates else ""
            if not correction_run and barcode_filename in existing_dat_barcodes:
                counts["skipped"] += len(candidates)
                _emit_progress(
                    progress_callback,
                    event="file_skipped",
                    phase="Importing DAT files",
                    message=f"Skipped existing DAT {candidates[0].name}",
                    current_file=str(candidates[0]),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
                continue
            try:
                selected_path, _selection_reason = _select_best_dat_candidate(connection, candidates)
            except FileNotFoundError:
                counts["skipped"] += len(candidates)
                counts["missing_files"] += len(candidates)
                _emit_progress(
                    progress_callback,
                    event="file_missing",
                    phase="Importing DAT files",
                    message=f"Skipped missing DAT candidate group for {candidates[0].name}",
                    current_file=str(candidates[0]),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
                continue
            current_source = _current_program_source(connection, selected_path.name)
            try:
                needs_import = (
                    current_source is None
                    or not _same_path(current_source, str(selected_path))
                    or should_process_file(connection, selected_path)
                )
            except FileNotFoundError:
                counts["skipped"] += 1
                counts["missing_files"] += 1
                _emit_progress(
                    progress_callback,
                    event="file_missing",
                    phase="Importing DAT files",
                    message=f"Skipped missing file {selected_path.name}",
                    current_file=str(selected_path),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
                continue

            _emit_progress(
                progress_callback,
                event="processing_file",
                phase="Importing DAT files",
                message=f"Checking {selected_path.name}",
                current_file=str(selected_path),
                current_step=step_index,
                total_steps=counts["total_steps"],
                processed=counts["processed"],
                skipped=counts["skipped"],
                errors=counts["errors"],
                missing_files=counts["missing_files"],
                filtered_old_files=counts["filtered_old_files"],
                total_supported_files=counts["total_supported_files"],
                nest_files=counts["nest_files"],
                dat_files=counts["dat_files"],
                dat_groups=counts["dat_groups"],
                duplicate_dat_files=counts["duplicate_dat_files"],
            )

            if needs_import:
                try:
                    import_file(connection, selected_path)
                except FileNotFoundError:
                    counts["skipped"] += 1
                    counts["missing_files"] += 1
                    _emit_progress(
                        progress_callback,
                        event="file_missing",
                        phase="Importing DAT files",
                        message=f"Skipped missing file {selected_path.name}",
                        current_file=str(selected_path),
                        current_step=step_index,
                        total_steps=counts["total_steps"],
                        processed=counts["processed"],
                        skipped=counts["skipped"],
                        errors=counts["errors"],
                        missing_files=counts["missing_files"],
                        filtered_old_files=counts["filtered_old_files"],
                        total_supported_files=counts["total_supported_files"],
                        nest_files=counts["nest_files"],
                        dat_files=counts["dat_files"],
                        dat_groups=counts["dat_groups"],
                        duplicate_dat_files=counts["duplicate_dat_files"],
                    )
                    continue
                counts["processed"] += 1
                changed_dat_barcodes.add(selected_path.name)
                existing_dat_barcodes.add(selected_path.name.upper())
                _emit_progress(
                    progress_callback,
                    event="file_processed",
                    phase="Importing DAT files",
                    message=f"Imported {selected_path.name}",
                    current_file=str(selected_path),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )
            else:
                counts["skipped"] += 1
                _emit_progress(
                    progress_callback,
                    event="file_skipped",
                    phase="Importing DAT files",
                    message=f"Skipped unchanged file {selected_path.name}",
                    current_file=str(selected_path),
                    current_step=step_index,
                    total_steps=counts["total_steps"],
                    processed=counts["processed"],
                    skipped=counts["skipped"],
                    errors=counts["errors"],
                    missing_files=counts["missing_files"],
                    filtered_old_files=counts["filtered_old_files"],
                    total_supported_files=counts["total_supported_files"],
                    nest_files=counts["nest_files"],
                    dat_files=counts["dat_files"],
                    dat_groups=counts["dat_groups"],
                    duplicate_dat_files=counts["duplicate_dat_files"],
                )

            # Count non-selected duplicates as skipped; they are intentionally not canonical for this barcode.
            for candidate in candidates:
                if candidate != selected_path:
                    counts["skipped"] += 1

        step_index += 1
        if correction_run and nest_comparison_changed:
            _emit_progress(
                progress_callback,
                event="rebuild_started",
                phase="Rebuilding resolved parts",
                message="Correction run rebuilding all resolved parts.",
                current_file="",
                current_step=step_index,
                total_steps=counts["total_steps"],
                processed=counts["processed"],
                skipped=counts["skipped"],
                errors=counts["errors"],
                missing_files=counts["missing_files"],
                filtered_old_files=counts["filtered_old_files"],
                total_supported_files=counts["total_supported_files"],
                nest_files=counts["nest_files"],
                dat_files=counts["dat_files"],
                dat_groups=counts["dat_groups"],
                duplicate_dat_files=counts["duplicate_dat_files"],
            )
            rebuild_resolved_nest_parts(connection)
        elif changed_dat_barcodes:
            _emit_progress(
                progress_callback,
                event="rebuild_started",
                phase="Rebuilding resolved parts",
                message=(
                    "Correction run refreshing resolved parts for changed DAT files."
                    if correction_run
                    else "Refreshing resolved parts for newly imported DAT files."
                ),
                current_file="",
                current_step=step_index,
                total_steps=counts["total_steps"],
                processed=counts["processed"],
                skipped=counts["skipped"],
                errors=counts["errors"],
                missing_files=counts["missing_files"],
                filtered_old_files=counts["filtered_old_files"],
                total_supported_files=counts["total_supported_files"],
                nest_files=counts["nest_files"],
                dat_files=counts["dat_files"],
                dat_groups=counts["dat_groups"],
                duplicate_dat_files=counts["duplicate_dat_files"],
            )
            rebuild_resolved_nest_parts_for_barcodes(connection, changed_dat_barcodes)
        connection.commit()
        _emit_progress(
            progress_callback,
            event="finished",
            phase="Complete",
            message="Import complete.",
            current_file="",
            current_step=step_index,
            total_steps=counts["total_steps"],
            processed=counts["processed"],
            skipped=counts["skipped"],
            errors=counts["errors"],
            missing_files=counts["missing_files"],
            filtered_old_files=counts["filtered_old_files"],
            total_supported_files=counts["total_supported_files"],
            nest_files=counts["nest_files"],
            dat_files=counts["dat_files"],
            dat_groups=counts["dat_groups"],
            duplicate_dat_files=counts["duplicate_dat_files"],
        )
        return counts


def import_paths(
    roots: Iterable[Path],
    progress_callback: ImportProgressCallback | None = None,
    *,
    changed_since: datetime | None = None,
) -> dict[str, Any]:
    return _import_paths_internal(
        roots,
        progress_callback=progress_callback,
        correction_run=False,
        changed_since=changed_since,
    )


def correction_import_paths(
    roots: Iterable[Path],
    progress_callback: ImportProgressCallback | None = None,
) -> dict[str, Any]:
    return _import_paths_internal(roots, progress_callback=progress_callback, correction_run=True)


def import_test_data(root: Path) -> dict[str, int]:
    return import_paths([root])
