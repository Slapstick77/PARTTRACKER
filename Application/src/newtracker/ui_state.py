from __future__ import annotations

import re
import shutil
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import DATA_DIR, get_connection
from .importer import is_ignored_source_path
from .persistence import atomic_write_json, read_json_file
from .schema import create_schema

LEGACY_UI_STATE_PATH = DATA_DIR / "ui_scan_state.json"
LEGACY_COMPLETED_LIST_PATH = DATA_DIR / "completed_scan_list.json"
LEGACY_MISSED_LIST_PATH = DATA_DIR / "missed_scan_list.json"
UI_SESSION_DIR = DATA_DIR / "ui_sessions"
LEGACY_MIGRATION_MARKER = UI_SESSION_DIR / ".legacy-migrated.json"
PART_TRACKER_MIGRATION_MARKER = DATA_DIR / ".part-tracker-migrated.json"
_UI_STATE_LOCK = threading.RLock()

TRACKER_STAGE_PROG = "Prog"
TRACKER_STAGE_CUT = "Cut"
TRACKER_STAGE_FORMED = "Formed"
TRACKER_STAGE_MISSING = "Missing"

class UiStateError(ValueError):
    pass


class UiStateStore:
    def __init__(self, session_key: str | None = None, path: Path | None = None) -> None:
        safe_session_key = re.sub(r"[^A-Za-z0-9_-]", "", session_key or "") or "shared"
        self.session_key = safe_session_key
        self.session_dir = UI_SESSION_DIR / safe_session_key
        self.path = path or (self.session_dir / "ui_scan_state.json")
        self.completed_path = self.session_dir / "completed_scan_list.json"
        self.missed_path = self.session_dir / "missed_scan_list.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate_legacy_state_if_needed()
        self._migrate_legacy_tracker_to_db_if_needed()
        if not self.path.exists():
            self.reset()
        if not self.completed_path.exists():
            self._write_completed([])
        if not self.missed_path.exists():
            self._write_missed([])

    def _migrate_legacy_state_if_needed(self) -> None:
        with _UI_STATE_LOCK:
            if LEGACY_MIGRATION_MARKER.exists():
                return

            migrated = False
            if LEGACY_UI_STATE_PATH.exists() and not self.path.exists():
                payload = read_json_file(LEGACY_UI_STATE_PATH, self._default_state, quarantine_corrupt=True)
                state = payload if isinstance(payload, dict) else self._default_state()
                atomic_write_json(self.path, state)
                migrated = True

            if LEGACY_COMPLETED_LIST_PATH.exists() and not self.completed_path.exists():
                payload = read_json_file(LEGACY_COMPLETED_LIST_PATH, list, quarantine_corrupt=True)
                rows = payload if isinstance(payload, list) else []
                atomic_write_json(self.completed_path, rows)
                migrated = True

            if LEGACY_MISSED_LIST_PATH.exists() and not self.missed_path.exists():
                payload = read_json_file(LEGACY_MISSED_LIST_PATH, list, quarantine_corrupt=True)
                rows = payload if isinstance(payload, list) else []
                atomic_write_json(self.missed_path, rows)
                migrated = True

            if migrated:
                atomic_write_json(
                    LEGACY_MIGRATION_MARKER,
                    {
                        "migrated_at": datetime.now().isoformat(timespec="seconds"),
                        "session_key": self.session_key,
                    },
                )

    @classmethod
    def clear_all_persisted_state(cls) -> None:
        with _UI_STATE_LOCK:
            if UI_SESSION_DIR.exists():
                shutil.rmtree(UI_SESSION_DIR, ignore_errors=True)
            if PART_TRACKER_MIGRATION_MARKER.exists():
                PART_TRACKER_MIGRATION_MARKER.unlink()
            for legacy_path in (LEGACY_UI_STATE_PATH, LEGACY_COMPLETED_LIST_PATH, LEGACY_MISSED_LIST_PATH):
                if legacy_path.exists():
                    legacy_path.unlink()

    def _default_state(self) -> dict[str, Any]:
        return {
            "machine_code": "",
            "user_code": "",
            "location_code": "",
            "update_target": "",
            "nest_data": "",
            "flat_scan_session_id": None,
            "flat_scan_status": "",
            "current_run_number": 0,
            "repeat_scan_pending": False,
            "pending_repeat_dat": "",
            "pending_repeat_run_number": None,
            "active_field": "machine_code",
            "expected_parts": [],
            "scanned_parts": [],
            "scan_edit_mode": False,
            "message": "Enter or scan MACHINE",
            "message_level": "info",
            "formed_queue": [],
            "formed_active_lists": [],
            "formed_active_field": "dat_token",
            "formed_message": "Scan DAT or formed part to load formed list",
            "formed_message_level": "info",
            "formed_selection_part": "",
            "formed_selection_candidates": [],
        }

    def read(self) -> dict[str, Any]:
        with _UI_STATE_LOCK:
            payload = read_json_file(self.path, self._default_state, quarantine_corrupt=True)
            if not isinstance(payload, dict):
                return self._default_state()
            payload.pop("monitor_units", None)
            return payload

    def write(self, state: dict[str, Any]) -> None:
        with _UI_STATE_LOCK:
            persisted = dict(state)
            persisted.pop("monitor_units", None)
            atomic_write_json(self.path, persisted)

    def _read_completed(self) -> list[dict[str, Any]]:
        with _UI_STATE_LOCK:
            payload = read_json_file(self.completed_path, list, quarantine_corrupt=True)
            return payload if isinstance(payload, list) else []

    def _write_completed(self, rows: list[dict[str, Any]]) -> None:
        with _UI_STATE_LOCK:
            atomic_write_json(self.completed_path, rows)

    def _read_missed(self) -> list[dict[str, Any]]:
        with _UI_STATE_LOCK:
            payload = read_json_file(self.missed_path, list, quarantine_corrupt=True)
            return payload if isinstance(payload, list) else []

    def _write_missed(self, rows: list[dict[str, Any]]) -> None:
        with _UI_STATE_LOCK:
            atomic_write_json(self.missed_path, rows)

    @staticmethod
    def _normalize_tracker_stage(stage: str | None, *, requires_forming: bool = False) -> str:
        raw = str(stage or "").strip().lower()
        if raw == TRACKER_STAGE_MISSING.lower():
            return TRACKER_STAGE_MISSING
        if raw == TRACKER_STAGE_FORMED.lower():
            return TRACKER_STAGE_FORMED
        if raw == TRACKER_STAGE_CUT.lower() or raw == "complete":
            return TRACKER_STAGE_CUT
        if raw == TRACKER_STAGE_PROG.lower() or raw == "in progress":
            return TRACKER_STAGE_PROG
        return TRACKER_STAGE_CUT if not raw and not requires_forming else TRACKER_STAGE_PROG

    @staticmethod
    def _tracker_stage_class(stage: str, requires_forming: bool) -> str:
        normalized = UiStateStore._normalize_tracker_stage(stage, requires_forming=requires_forming)
        if normalized == TRACKER_STAGE_MISSING:
            return "stage-missing"
        if normalized == TRACKER_STAGE_FORMED:
            return "stage-formed"
        if normalized == TRACKER_STAGE_CUT:
            return "stage-cut-formed" if requires_forming else "stage-cut-complete"
        return "stage-prog"

    @staticmethod
    def _history_group_key(dat_name: str, nest_part_id: int | None, sequence: int) -> str:
        dat_token = str(dat_name or "").strip().upper()
        nest_token = "legacy" if nest_part_id is None else str(int(nest_part_id))
        return f"{dat_token}|{nest_token}|{int(sequence)}"

    @staticmethod
    def _history_signature(row: Any) -> tuple[Any, ...]:
        return (
            str(row["dat_name"] or ""),
            int(row["run_number"] or 1),
            int(row["scan_sequence"] or 1),
            str(row["part_number"] or ""),
            str(row["part_revision"] or "-"),
            str(row["com_number"] or ""),
            str(row["machine"] or ""),
            str(row["user_code"] or ""),
            str(row["location"] or ""),
            int(row["requires_forming"] or 0),
            str(row["stage"] or TRACKER_STAGE_PROG),
        )

    @staticmethod
    def _history_event_label(event_type: str) -> str:
        labels = {
            "baseline": "Baseline",
            "main_progress": "Main DAT Scan",
            "main_complete": "Main Complete",
            "main_force_complete": "Main Force Complete",
            "main_force_missing": "Main Missing",
            "formed_complete": "Formed Complete",
            "formed_force_complete": "Formed Force Complete",
            "formed_force_missing": "Formed Missing",
        }
        return labels.get(event_type, event_type.replace("_", " ").title())

    def _record_tracker_history(
        self,
        connection,
        tracker_keys: list[str],
        *,
        event_type: str,
        scanner_name: str,
        notes: str = "",
    ) -> None:
        unique_keys = [key for key in dict.fromkeys(str(key or "").strip() for key in tracker_keys) if key]
        if not unique_keys:
            return

        placeholders = ",".join("?" for _ in unique_keys)
        rows = connection.execute(
            f"""
            SELECT
                tracker_key,
                dat_name,
                run_number,
                nest_part_id,
                scan_sequence,
                part_number,
                part_revision,
                com_number,
                machine,
                user_code,
                location,
                requires_forming,
                stage,
                stage_updated_at,
                updated_at,
                created_at
            FROM part_tracker_items
            WHERE tracker_key IN ({placeholders})
            """,
            tuple(unique_keys),
        ).fetchall()

        inserts: list[tuple[Any, ...]] = []
        for row in rows:
            last_row = connection.execute(
                """
                SELECT
                    dat_name,
                    run_number,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    event_type,
                    scanner_name
                FROM part_tracker_history
                WHERE tracker_key = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(row["tracker_key"]),),
            ).fetchone()
            if (
                last_row is not None
                and self._history_signature(last_row) == self._history_signature(row)
                and str(last_row["event_type"] or "") == event_type
                and str(last_row["scanner_name"] or "") == scanner_name
            ):
                continue

            dat_name = str(row["dat_name"] or "")
            nest_part_id = int(row["nest_part_id"]) if row["nest_part_id"] is not None else None
            sequence = int(row["scan_sequence"] or 1)
            inserts.append(
                (
                    str(row["tracker_key"]),
                    self._history_group_key(dat_name, nest_part_id, sequence),
                    event_type,
                    scanner_name,
                    dat_name,
                    int(row["run_number"] or 1),
                    nest_part_id,
                    sequence,
                    str(row["part_number"] or ""),
                    str(row["part_revision"] or "-"),
                    str(row["com_number"] or ""),
                    str(row["machine"] or ""),
                    str(row["user_code"] or ""),
                    str(row["location"] or ""),
                    1 if bool(row["requires_forming"]) else 0,
                    str(row["stage"] or TRACKER_STAGE_PROG),
                    str(row["stage_updated_at"] or row["updated_at"] or row["created_at"] or datetime.now().isoformat(timespec="seconds")),
                    notes,
                )
            )

        if inserts:
            connection.executemany(
                """
                INSERT INTO part_tracker_history (
                    tracker_key,
                    history_group_key,
                    event_type,
                    scanner_name,
                    dat_name,
                    run_number,
                    nest_part_id,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    recorded_at,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserts,
            )

    @staticmethod
    def _tracker_key(dat_name: str, run_number: int, nest_part_id: int | None, sequence: int) -> str:
        dat_token = str(dat_name or "").strip().upper()
        nest_token = "legacy" if nest_part_id is None else str(int(nest_part_id))
        return f"{dat_token}|run{int(run_number)}|{nest_token}|{int(sequence)}"

    @staticmethod
    def _clear_repeat_scan_prompt(state: dict[str, Any]) -> None:
        state["repeat_scan_pending"] = False
        state["pending_repeat_dat"] = ""
        state["pending_repeat_run_number"] = None

    def _legacy_tracker_stage(self, row: dict[str, Any], default_stage: str) -> str:
        requires_forming = bool(row.get("f_flag") or row.get("requires_forming"))
        stage = str(row.get("stage") or default_stage or "").strip()
        return self._normalize_tracker_stage(stage, requires_forming=requires_forming)

    def _migrate_legacy_tracker_to_db_if_needed(self) -> None:
        with _UI_STATE_LOCK:
            if PART_TRACKER_MIGRATION_MARKER.exists():
                return

            source_specs: list[tuple[Path, str]] = []
            if LEGACY_COMPLETED_LIST_PATH.exists():
                source_specs.append((LEGACY_COMPLETED_LIST_PATH, TRACKER_STAGE_CUT))
            if LEGACY_MISSED_LIST_PATH.exists():
                source_specs.append((LEGACY_MISSED_LIST_PATH, TRACKER_STAGE_MISSING))
            if UI_SESSION_DIR.exists():
                for session_dir in UI_SESSION_DIR.iterdir():
                    if not session_dir.is_dir():
                        continue
                    completed_path = session_dir / "completed_scan_list.json"
                    missed_path = session_dir / "missed_scan_list.json"
                    if completed_path.exists():
                        source_specs.append((completed_path, TRACKER_STAGE_CUT))
                    if missed_path.exists():
                        source_specs.append((missed_path, TRACKER_STAGE_MISSING))

            migrated_rows = 0
            now = datetime.now().isoformat(timespec="seconds")
            if not source_specs:
                atomic_write_json(
                    PART_TRACKER_MIGRATION_MARKER,
                    {
                        "migrated_at": now,
                        "rows": 0,
                    },
                )
                return

            try:
                with get_connection() as connection:
                    create_schema(connection)
                    for source_path, default_stage in source_specs:
                        payload = read_json_file(source_path, list, quarantine_corrupt=True)
                        rows = payload if isinstance(payload, list) else []
                        if not rows:
                            continue

                        params: list[tuple[Any, ...]] = []
                        for index, raw_row in enumerate(rows):
                            if not isinstance(raw_row, dict):
                                continue
                            stage = self._legacy_tracker_stage(raw_row, default_stage)
                            requires_forming = 1 if bool(raw_row.get("f_flag") or raw_row.get("requires_forming")) else 0
                            timestamp = str(
                                raw_row.get("stage_updated_at")
                                or raw_row.get("completed_at")
                                or raw_row.get("updated_at")
                                or raw_row.get("created_at")
                                or now
                            )
                            tracker_key = f"legacy|{source_path.as_posix()}|{index}"
                            params.append(
                                (
                                    tracker_key,
                                    None,
                                    int(raw_row.get("run_number") or 1),
                                    str(raw_row.get("nest_data") or raw_row.get("dat_name") or ""),
                                    None,
                                    int(raw_row.get("sequence") or index + 1),
                                    str(raw_row.get("part_number") or ""),
                                    str(raw_row.get("part_revision") or "-"),
                                    str(raw_row.get("com_number") or ""),
                                    str(raw_row.get("machine") or ""),
                                    str(raw_row.get("user") or raw_row.get("user_code") or ""),
                                    str(raw_row.get("location") or ""),
                                    requires_forming,
                                    stage,
                                    timestamp,
                                    timestamp,
                                    timestamp,
                                )
                            )

                        if not params:
                            continue

                        connection.executemany(
                            """
                            INSERT INTO part_tracker_items (
                                tracker_key,
                                flat_scan_session_id,
                                run_number,
                                dat_name,
                                nest_part_id,
                                scan_sequence,
                                part_number,
                                part_revision,
                                com_number,
                                machine,
                                user_code,
                                location,
                                requires_forming,
                                stage,
                                stage_updated_at,
                                created_at,
                                updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(tracker_key) DO NOTHING
                            """,
                            params,
                        )
                        migrated_rows += len(params)

                    connection.commit()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                return

            atomic_write_json(
                PART_TRACKER_MIGRATION_MARKER,
                {
                    "migrated_at": now,
                    "rows": migrated_rows,
                },
            )

    def _apply_state_defaults_to_parts(self, state: dict[str, Any]) -> None:
        machine = str(state.get("machine_code") or "")
        user_code = str(state.get("user_code") or "")
        location = str(state.get("location_code") or "")
        for key in ("expected_parts", "scanned_parts"):
            normalized_parts: list[dict[str, Any]] = []
            for raw_part in state.get(key, []):
                part = dict(raw_part)
                part.setdefault("machine", machine)
                part.setdefault("user_code", user_code)
                part.setdefault("location", location)
                normalized_parts.append(part)
            state[key] = normalized_parts

    def _tracker_progress_rows(self, state: dict[str, Any]) -> list[tuple[Any, ...]]:
        session_id = state.get("flat_scan_session_id")
        now = datetime.now().isoformat(timespec="seconds")
        params: list[tuple[Any, ...]] = []
        for part in list(state.get("expected_parts", [])) + list(state.get("scanned_parts", [])):
            dat_name = str(part.get("dat_name") or state.get("nest_data") or "").strip().upper()
            nest_part_id = int(part["nest_part_id"])
            sequence = int(part.get("sequence") or 1)
            run_number = int(part.get("run_number") or state.get("current_run_number") or 1)
            machine = str(part.get("machine") or state.get("machine_code") or "")
            user_code = str(part.get("user_code") or state.get("user_code") or "")
            location = str(part.get("location") or state.get("location_code") or "")
            params.append(
                (
                    self._tracker_key(dat_name, run_number, nest_part_id, sequence),
                    int(session_id) if session_id is not None else None,
                    run_number,
                    dat_name,
                    nest_part_id,
                    sequence,
                    str(part.get("part_number") or ""),
                    str(part.get("part_revision") or "-"),
                    str(part.get("com_number") or ""),
                    machine,
                    user_code,
                    location,
                    1 if bool(part.get("requires_forming")) else 0,
                    TRACKER_STAGE_PROG,
                    now,
                    now,
                    now,
                )
            )
        return params

    def _sync_part_tracker_progress(self, state: dict[str, Any]) -> None:
        params = self._tracker_progress_rows(state)
        if not params:
            return

        with get_connection() as connection:
            create_schema(connection)
            connection.executemany(
                """
                INSERT INTO part_tracker_items (
                    tracker_key,
                    flat_scan_session_id,
                    run_number,
                    dat_name,
                    nest_part_id,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    stage_updated_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tracker_key) DO UPDATE SET
                    flat_scan_session_id = COALESCE(excluded.flat_scan_session_id, part_tracker_items.flat_scan_session_id),
                    run_number = excluded.run_number,
                    dat_name = excluded.dat_name,
                    nest_part_id = COALESCE(excluded.nest_part_id, part_tracker_items.nest_part_id),
                    scan_sequence = excluded.scan_sequence,
                    part_revision = excluded.part_revision,
                    requires_forming = excluded.requires_forming,
                    part_number = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.part_number
                        ELSE part_tracker_items.part_number
                    END,
                    com_number = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.com_number
                        ELSE part_tracker_items.com_number
                    END,
                    machine = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.machine
                        ELSE part_tracker_items.machine
                    END,
                    user_code = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.user_code
                        ELSE part_tracker_items.user_code
                    END,
                    location = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.location
                        ELSE part_tracker_items.location
                    END,
                    stage_updated_at = CASE
                        WHEN part_tracker_items.stage = 'Prog' THEN excluded.stage_updated_at
                        ELSE part_tracker_items.stage_updated_at
                    END,
                    updated_at = excluded.updated_at
                """,
                params,
            )
            self._record_tracker_history(
                connection,
                [str(item[0]) for item in params],
                event_type="main_progress",
                scanner_name="main",
                notes="Main DAT scan synchronized to tracker.",
            )
            connection.commit()

    def _tracker_stage_rows(self, state: dict[str, Any], parts: list[dict[str, Any]], stage: str) -> list[tuple[Any, ...]]:
        session_id = state.get("flat_scan_session_id")
        now = datetime.now().isoformat(timespec="seconds")
        params: list[tuple[Any, ...]] = []
        for part in parts:
            dat_name = str(part.get("dat_name") or state.get("nest_data") or "").strip().upper()
            nest_part_id = int(part["nest_part_id"])
            sequence = int(part.get("sequence") or 1)
            run_number = int(part.get("run_number") or state.get("current_run_number") or 1)
            machine = str(part.get("machine") or state.get("machine_code") or "")
            user_code = str(part.get("user_code") or state.get("user_code") or "")
            location = str(part.get("location") or state.get("location_code") or "")
            params.append(
                (
                    self._tracker_key(dat_name, run_number, nest_part_id, sequence),
                    int(session_id) if session_id is not None else None,
                    run_number,
                    dat_name,
                    nest_part_id,
                    sequence,
                    str(part.get("part_number") or ""),
                    str(part.get("part_revision") or "-"),
                    str(part.get("com_number") or ""),
                    machine,
                    user_code,
                    location,
                    1 if bool(part.get("requires_forming")) else 0,
                    stage,
                    now,
                    now,
                    now,
                )
            )
        return params

    def _apply_tracker_stage(
        self,
        state: dict[str, Any],
        parts: list[dict[str, Any]],
        stage: str,
        *,
        event_type: str,
        scanner_name: str,
        notes: str = "",
    ) -> int:
        params = self._tracker_stage_rows(state, parts, stage)
        if not params:
            return 0

        with get_connection() as connection:
            create_schema(connection)
            connection.executemany(
                """
                INSERT INTO part_tracker_items (
                    tracker_key,
                    flat_scan_session_id,
                    run_number,
                    dat_name,
                    nest_part_id,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    stage_updated_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tracker_key) DO UPDATE SET
                    flat_scan_session_id = COALESCE(excluded.flat_scan_session_id, part_tracker_items.flat_scan_session_id),
                    run_number = excluded.run_number,
                    dat_name = excluded.dat_name,
                    nest_part_id = COALESCE(excluded.nest_part_id, part_tracker_items.nest_part_id),
                    scan_sequence = excluded.scan_sequence,
                    part_number = excluded.part_number,
                    part_revision = excluded.part_revision,
                    com_number = excluded.com_number,
                    machine = excluded.machine,
                    user_code = excluded.user_code,
                    location = excluded.location,
                    requires_forming = excluded.requires_forming,
                    stage = excluded.stage,
                    stage_updated_at = excluded.stage_updated_at,
                    updated_at = excluded.updated_at
                """,
                params,
            )
            self._record_tracker_history(
                connection,
                [str(item[0]) for item in params],
                event_type=event_type,
                scanner_name=scanner_name,
                notes=notes,
            )
            connection.commit()
        return len(params)

    def start_scan_edit(self) -> dict[str, Any]:
        state = self.read()
        if not state.get("nest_data"):
            raise UiStateError("Scan NEST DATA before editing scanned parts.")
        if not state.get("scanned_parts"):
            raise UiStateError("Scan at least one part before editing the scanned list.")
        state["scan_edit_mode"] = True
        state["message"] = "Edit scanned part details, then click Done."
        state["message_level"] = "info"
        self.write(state)
        return state

    def save_scan_edits(self, form) -> dict[str, Any]:
        state = self.read()
        if not state.get("scan_edit_mode"):
            return state

        updated_parts: list[dict[str, Any]] = []
        for index, raw_part in enumerate(state.get("scanned_parts", [])):
            part = dict(raw_part)
            part_number = str(form.get(f"scanned_{index}_part_number", part.get("part_number", "")) or "").strip()
            com_number = str(form.get(f"scanned_{index}_com_number", part.get("com_number", "")) or "").strip()
            location = str(form.get(f"scanned_{index}_location", part.get("location", state.get("location_code", ""))) or "").strip()
            if not part_number:
                raise UiStateError("Part number cannot be blank in edit mode.")
            part["part_number"] = part_number
            part["com_number"] = com_number
            part["location"] = location
            updated_parts.append(part)

        state["scanned_parts"] = updated_parts
        state["scan_edit_mode"] = False
        state["message"] = "Scanned part edits saved to this browser session. Click Complete or Force Complete to submit them."
        state["message_level"] = "success"
        self.write(state)
        return state

    def _reset_after_batch_submission(self, state: dict[str, Any], message: str) -> dict[str, Any]:
        reset_state = self._default_state()
        reset_state["machine_code"] = state.get("machine_code", "")
        reset_state["user_code"] = state.get("user_code", "")
        reset_state["location_code"] = state.get("location_code", "")
        reset_state["formed_queue"] = state.get("formed_queue", [])
        reset_state["formed_active_lists"] = state.get("formed_active_lists", [])
        reset_state["formed_active_field"] = state.get("formed_active_field", "dat_token")
        reset_state["formed_message"] = state.get("formed_message", "Scan DAT or formed part to load formed list")
        reset_state["formed_message_level"] = state.get("formed_message_level", "info")
        reset_state["formed_selection_part"] = state.get("formed_selection_part", "")
        reset_state["formed_selection_candidates"] = list(state.get("formed_selection_candidates", []))
        if reset_state["machine_code"] and reset_state["user_code"] and reset_state["location_code"]:
            reset_state["active_field"] = "nest_data"
        else:
            reset_state["active_field"] = "machine_code"
        reset_state["message"] = message
        reset_state["message_level"] = "success"
        self.write(reset_state)
        return reset_state

    def _tracker_row_payload(self, row: Any) -> dict[str, Any]:
        requires_forming = bool(row["requires_forming"])
        stage = self._normalize_tracker_stage(str(row["stage"] or ""), requires_forming=requires_forming)
        run_number = int(row["run_number"] or 1)
        latest_run_number = int(row["latest_run_number"] or run_number)
        return {
            "tracker_key": str(row["tracker_key"]),
            "updated_at": str(row["stage_updated_at"] or row["updated_at"] or row["created_at"] or ""),
            "run_number": run_number,
            "run_class": "run-pill-latest" if run_number >= latest_run_number else "run-pill-older",
            "machine": str(row["machine"] or ""),
            "user": str(row["user_code"] or ""),
            "location": str(row["location"] or ""),
            "nest_data": str(row["dat_name"] or ""),
            "part_number": str(row["part_number"] or ""),
            "part_revision": str(row["part_revision"] or "-"),
            "com_number": str(row["com_number"] or ""),
            "f_flag": requires_forming,
            "stage": stage,
            "stage_class": self._tracker_stage_class(stage, requires_forming),
        }

    def _history_row_payload(self, row: Any) -> dict[str, Any]:
        requires_forming = bool(row["requires_forming"])
        stage = self._normalize_tracker_stage(str(row["stage"] or ""), requires_forming=requires_forming)
        return {
            "recorded_at": str(row["recorded_at"] or ""),
            "event_label": self._history_event_label(str(row["event_type"] or "")),
            "scanner_name": str(row["scanner_name"] or ""),
            "run_number": int(row["run_number"] or 1),
            "stage": stage,
            "stage_class": self._tracker_stage_class(stage, requires_forming),
            "part_number": str(row["part_number"] or ""),
            "part_revision": str(row["part_revision"] or "-"),
            "com_number": str(row["com_number"] or ""),
            "machine": str(row["machine"] or ""),
            "user": str(row["user_code"] or ""),
            "location": str(row["location"] or ""),
            "f_flag": requires_forming,
            "notes": str(row["notes"] or ""),
        }

    def reset(self) -> dict[str, Any]:
        previous = self.read() if self.path.exists() else self._default_state()
        state = self._default_state()
        state["machine_code"] = previous.get("machine_code", "")
        state["user_code"] = previous.get("user_code", "")
        state["location_code"] = previous.get("location_code", "")

        if state["machine_code"] and state["user_code"] and state["location_code"]:
            state["active_field"] = "nest_data"
            state["message"] = "Scan NEST DATA"
        elif state["machine_code"] and state["user_code"]:
            state["active_field"] = "location_code"
            state["message"] = "Scan LOCATION"
        elif state["machine_code"]:
            state["active_field"] = "user_code"
            state["message"] = "Scan USER"

        self.write(state)
        return state

    def begin_update(self, field_name: str) -> dict[str, Any]:
        if field_name not in {"machine_code", "user_code", "location_code"}:
            raise UiStateError(f"Unsupported update field {field_name}")

        state = self.read()
        state["update_target"] = field_name
        state["active_field"] = field_name
        label = {
            "machine_code": "MACHINE",
            "user_code": "USER",
            "location_code": "LOCATION",
        }[field_name]
        state["message"] = f"Update mode: scan new {label}"
        state["message_level"] = "info"
        self.write(state)
        return state

    def _load_repeat_scan_info(self, connection, dat_name: str, rows: list[Any]) -> dict[str, Any]:
        if not rows:
            raise UiStateError(f"No resolved parts found for {dat_name}")

        nest_id = int(rows[0]["nest_id"])
        session_row = connection.execute(
            """
            SELECT id, status, COALESCE(run_number, 1) AS run_number
            FROM flat_scan_sessions
            WHERE nest_id = ?
            ORDER BY COALESCE(run_number, 1) DESC, id DESC
            LIMIT 1
            """,
            (nest_id,),
        ).fetchone()
        tracker_row = connection.execute(
            """
            SELECT COALESCE(MAX(run_number), 0) AS max_finalized_run
            FROM part_tracker_items
            WHERE UPPER(TRIM(dat_name)) = UPPER(TRIM(?))
              AND stage IN ('Cut', 'Missing')
            """,
            (dat_name,),
        ).fetchone()
        max_finalized_run = int(tracker_row["max_finalized_run"] or 0) if tracker_row is not None else 0
        if session_row is None:
            return {
                "has_completed_run": max_finalized_run > 0,
                "next_run_number": max(1, max_finalized_run + 1),
                "latest_status": "",
            }

        latest_status = str(session_row["status"] or "")
        latest_run_number = int(session_row["run_number"] or 1)
        latest_known_run = max(latest_run_number, max_finalized_run)
        resumable_open_run = latest_status != "completed" and latest_run_number > max_finalized_run
        return {
            "has_completed_run": (latest_status == "completed") or (max_finalized_run > 0 and not resumable_open_run),
            "next_run_number": latest_known_run + 1,
            "latest_status": latest_status,
        }

    def _load_resolved_rows_for_dat(
        self,
        connection,
        dat_name: str,
        *,
        requires_forming: bool | None = None,
    ):
        query = """
            SELECT
                r.nest_id,
                r.nest_part_id,
                r.barcode_filename,
                r.part_number,
                r.part_revision,
                r.quantity_nested,
                r.com_number,
                COALESCE(r.requires_forming, 0) AS requires_forming,
                r.matched_part_attribute_id,
                pn.build_date_code
            FROM resolved_nest_parts r
            JOIN program_nests pn ON pn.id = r.nest_id
            WHERE r.barcode_filename = ?
        """
        params: list[Any] = [dat_name]
        if requires_forming is not None:
            query += " AND COALESCE(r.requires_forming, 0) = ?"
            params.append(1 if requires_forming else 0)
        query += " ORDER BY r.part_number, r.nest_part_id"
        return connection.execute(query, tuple(params)).fetchall()

    @staticmethod
    def _build_part_instance(row: Any, sequence: int, **extra: Any) -> dict[str, Any]:
        payload = {
            "nest_part_id": int(row["nest_part_id"]),
            "part_attribute_id": row["matched_part_attribute_id"],
            "part_number": row["part_number"],
            "part_revision": row["part_revision"] or "-",
            "com_number": row["com_number"],
            "requires_forming": bool(row["requires_forming"]),
            "sequence": sequence,
            "dat_name": row["barcode_filename"],
        }
        payload.update(extra)
        return payload

    @staticmethod
    def _build_tracker_part_instance(row: Any, **extra: Any) -> dict[str, Any]:
        payload = {
            "tracker_key": str(row["tracker_key"]),
            "flat_scan_session_id": int(row["flat_scan_session_id"]) if row["flat_scan_session_id"] is not None else None,
            "run_number": int(row["run_number"] or 1),
            "nest_part_id": int(row["nest_part_id"]),
            "part_number": str(row["part_number"] or ""),
            "part_revision": str(row["part_revision"] or "-"),
            "com_number": str(row["com_number"] or ""),
            "machine": str(row["machine"] or ""),
            "user_code": str(row["user_code"] or ""),
            "location": str(row["location"] or ""),
            "requires_forming": bool(row["requires_forming"]),
            "sequence": int(row["scan_sequence"] or 1),
            "dat_name": str(row["dat_name"] or ""),
            "stage": str(row["stage"] or TRACKER_STAGE_PROG),
        }
        payload.update(extra)
        return payload

    @staticmethod
    def _extract_com_numbers_from_rows(rows: list[Any]) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for row in rows:
            raw = str(row["com_number"] or "").strip()
            if not raw or raw in seen:
                continue
            seen.add(raw)
            ordered.append(raw)
        return ordered

    def _upsert_monitor_units(self, connection, rows: list[Any], *, dat_name: str | None = None) -> list[str]:
        com_numbers = self._extract_com_numbers_from_rows(rows)
        if not com_numbers:
            return []

        activity_at = datetime.now().isoformat(timespec="seconds")
        for com_number in com_numbers:
            connection.execute(
                """
                INSERT INTO monitor_units (com_number, status, started_at, last_activity_at)
                VALUES (?, 'in_progress', ?, ?)
                ON CONFLICT(com_number) DO UPDATE SET
                    status = 'in_progress',
                    last_activity_at = excluded.last_activity_at
                """,
                (com_number, activity_at, activity_at),
            )
            if dat_name:
                unit_row = connection.execute(
                    "SELECT id FROM monitor_units WHERE com_number = ?",
                    (com_number,),
                ).fetchone()
                if unit_row is not None:
                    connection.execute(
                        "INSERT OR IGNORE INTO monitor_unit_sources (monitor_unit_id, barcode_filename) VALUES (?, ?)",
                        (int(unit_row["id"]), dat_name),
                    )
        return com_numbers

    def _touch_monitor_units(self, connection, com_numbers: list[str], *, status: str | None = None) -> None:
        seen: set[str] = set()
        activity_at = datetime.now().isoformat(timespec="seconds")
        for raw_com in com_numbers:
            com_number = str(raw_com or "").strip()
            if not com_number or com_number in seen:
                continue
            seen.add(com_number)
            if status is None:
                connection.execute(
                    "UPDATE monitor_units SET last_activity_at = ? WHERE com_number = ?",
                    (activity_at, com_number),
                )
            else:
                connection.execute(
                    "UPDATE monitor_units SET status = ?, last_activity_at = ? WHERE com_number = ?",
                    (status, activity_at, com_number),
                )

    def _ensure_flat_scan_session(
        self,
        connection,
        dat_name: str,
        rows: list[Any],
        *,
        start_new_session: bool = False,
        forced_run_number: int | None = None,
    ) -> tuple[int, str, int]:
        if not rows:
            raise UiStateError(f"No resolved parts found for {dat_name}")

        nest_id = int(rows[0]["nest_id"])
        session_row = connection.execute(
            """
            SELECT id, status, COALESCE(run_number, 1) AS run_number
            FROM flat_scan_sessions
            WHERE nest_id = ?
            ORDER BY COALESCE(run_number, 1) DESC, id DESC
            LIMIT 1
            """,
            (nest_id,),
        ).fetchone()
        latest_run_number = int(session_row["run_number"] or 1) if session_row is not None else 0
        if session_row is None or start_new_session:
            started_at = datetime.now().isoformat(timespec="seconds")
            run_number = int(forced_run_number or (latest_run_number + 1) or 1)
            cursor = connection.execute(
                "INSERT INTO flat_scan_sessions (nest_id, run_number, started_at, status) VALUES (?, ?, ?, 'open')",
                (nest_id, run_number, started_at),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("Failed to create flat scan session.")
            session_id = int(cursor.lastrowid)
            session_status = "open"
        else:
            session_id = int(session_row["id"])
            session_status = str(session_row["status"] or "open")
            run_number = latest_run_number

        for row in rows:
            expected_quantity = max(1, int(row["quantity_nested"] or 0))
            requires_forming = 1 if bool(row["requires_forming"]) else 0
            connection.execute(
                """
                INSERT INTO flat_scan_items (
                    flat_scan_session_id,
                    nest_part_id,
                    expected_quantity,
                    scanned_quantity,
                    is_complete,
                    requires_forming
                ) VALUES (?, ?, ?, 0, 0, ?)
                ON CONFLICT(flat_scan_session_id, nest_part_id) DO UPDATE SET
                    expected_quantity = excluded.expected_quantity,
                    requires_forming = excluded.requires_forming,
                    is_complete = CASE
                        WHEN flat_scan_items.scanned_quantity >= excluded.expected_quantity THEN 1
                        ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (session_id, int(row["nest_part_id"]), expected_quantity, requires_forming),
            )

        return session_id, session_status, run_number

    def _load_flat_scan_snapshot(
        self,
        dat_name: str,
        *,
        start_new_session: bool = False,
        forced_run_number: int | None = None,
    ) -> dict[str, Any]:
        with get_connection() as connection:
            create_schema(connection)
            rows = self._load_resolved_rows_for_dat(connection, dat_name)
            if not rows:
                raise UiStateError(f"No resolved parts found for {dat_name}")
            self._upsert_monitor_units(connection, rows, dat_name=dat_name)
            session_id, session_status, run_number = self._ensure_flat_scan_session(
                connection,
                dat_name,
                rows,
                start_new_session=start_new_session,
                forced_run_number=forced_run_number,
            )
            item_rows = connection.execute(
                "SELECT nest_part_id, expected_quantity, scanned_quantity FROM flat_scan_items WHERE flat_scan_session_id = ?",
                (session_id,),
            ).fetchall()
            connection.commit()

        items_by_nest = {
            int(row["nest_part_id"]): row
            for row in item_rows
        }
        expected_parts: list[dict[str, Any]] = []
        scanned_parts: list[dict[str, Any]] = []
        for row in rows:
            item_row = items_by_nest.get(int(row["nest_part_id"]))
            expected_quantity = max(1, int((item_row["expected_quantity"] if item_row is not None else row["quantity_nested"]) or 0))
            scanned_quantity = int(item_row["scanned_quantity"] or 0) if item_row is not None else 0
            scanned_quantity = max(0, min(scanned_quantity, expected_quantity))

            for sequence in range(1, scanned_quantity + 1):
                scanned_parts.append(
                    self._build_part_instance(
                        row,
                        sequence,
                        flat_scan_session_id=session_id,
                        run_number=run_number,
                    )
                )
            for sequence in range(scanned_quantity + 1, expected_quantity + 1):
                expected_parts.append(
                    self._build_part_instance(
                        row,
                        sequence,
                        flat_scan_session_id=session_id,
                        run_number=run_number,
                    )
                )

        return {
            "session_id": session_id,
            "status": session_status,
            "run_number": run_number,
            "expected_parts": expected_parts,
            "scanned_parts": scanned_parts,
        }

    def _apply_flat_scan_snapshot(self, state: dict[str, Any], dat_name: str, snapshot: dict[str, Any]) -> None:
        state["nest_data"] = dat_name
        state["update_target"] = ""
        self._clear_repeat_scan_prompt(state)
        state["flat_scan_session_id"] = snapshot["session_id"]
        state["flat_scan_status"] = snapshot["status"]
        state["current_run_number"] = int(snapshot.get("run_number") or 1)
        state["expected_parts"] = list(snapshot["expected_parts"])
        state["scanned_parts"] = list(snapshot["scanned_parts"])
        state["scan_edit_mode"] = False
        self._apply_state_defaults_to_parts(state)
        self._sync_part_tracker_progress(state)
        self._queue_formed_from_nest(state, dat_name)
        if state["expected_parts"]:
            state["active_field"] = "part_scan"
            state["message"] = "Continue scanning parts" if state["scanned_parts"] else "Start scanning parts"
            state["message_level"] = "info"
        else:
            state["active_field"] = "nest_data"
            state["message"] = f"All parts already scanned for {dat_name}. Scan the next DAT."
            state["message_level"] = "success"

    def _increment_flat_scan_item(self, flat_scan_session_id: int | None, part: dict[str, Any]) -> None:
        self._increment_flat_scan_items(flat_scan_session_id, [part])

    def _increment_flat_scan_items(self, flat_scan_session_id: int | None, parts: list[dict[str, Any]]) -> None:
        if flat_scan_session_id is None or not parts:
            return

        update_params = [
            (int(flat_scan_session_id), int(part["nest_part_id"]))
            for part in parts
            if part.get("nest_part_id") is not None
        ]
        if not update_params:
            return

        event_params = [
            (
                "flat_scan",
                str(part.get("part_number") or ""),
                str(part.get("part_number") or ""),
                str(part.get("part_revision") or ""),
                int(flat_scan_session_id),
                str(part.get("com_number") or ""),
            )
            for part in parts
        ]
        com_numbers = [str(part.get("com_number") or "") for part in parts]

        with get_connection() as connection:
            create_schema(connection)
            connection.executemany(
                """
                UPDATE flat_scan_items
                SET scanned_quantity = CASE
                        WHEN scanned_quantity < expected_quantity THEN scanned_quantity + 1
                        ELSE scanned_quantity
                    END,
                    is_complete = CASE
                        WHEN scanned_quantity + 1 >= expected_quantity THEN 1
                        ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE flat_scan_session_id = ? AND nest_part_id = ?
                """,
                update_params,
            )
            connection.executemany(
                """
                INSERT INTO scan_events (
                    event_type,
                    barcode_value,
                    part_number,
                    part_revision,
                    flat_scan_session_id,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                event_params,
            )
            self._touch_monitor_units(connection, com_numbers)
            connection.commit()

    def _ensure_forming_batch(self, connection, dat_name: str, rows: list[Any], *, mark_started: bool) -> tuple[int, str, str, str, int]:
        if not rows:
            raise UiStateError(f"No forming parts found for {dat_name}")

        run_number = int(rows[0]["run_number"] or 1)
        batch_code = f"{dat_name}|run{run_number}"
        batch_row = connection.execute(
            "SELECT id, status, created_at, started_at FROM forming_batches WHERE batch_code = ?",
            (batch_code,),
        ).fetchone()
        if batch_row is None:
            com_numbers = self._extract_com_numbers_from_rows(rows)
            cursor = connection.execute(
                """
                INSERT INTO forming_batches (
                    batch_code,
                    source_nest_id,
                    com_number,
                    build_date_code,
                    status,
                    started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_code,
                    None,
                    com_numbers[0] if len(com_numbers) == 1 else None,
                    None,
                    "in_progress" if mark_started else "queued",
                    datetime.now().isoformat(timespec="seconds") if mark_started else None,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("Failed to create forming batch.")
            batch_id = int(cursor.lastrowid)
            batch_status = "in_progress" if mark_started else "queued"
            created_at = datetime.now().isoformat(timespec="seconds")
            started_at = created_at if mark_started else ""
        else:
            batch_id = int(batch_row["id"])
            batch_status = str(batch_row["status"] or "queued")
            created_at = str(batch_row["created_at"] or "")
            started_at = str(batch_row["started_at"] or "")

        rows_by_nest: dict[int, list[Any]] = {}
        for row in rows:
            rows_by_nest.setdefault(int(row["nest_part_id"]), []).append(row)

        for nest_part_id, grouped_rows in rows_by_nest.items():
            reference_row = grouped_rows[0]
            expected_quantity = len(grouped_rows)
            connection.execute(
                """
                INSERT INTO forming_batch_items (
                    forming_batch_id,
                    nest_part_id,
                    part_attribute_id,
                    part_number,
                    part_revision,
                    expected_quantity,
                    scanned_quantity,
                    is_complete
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                ON CONFLICT(forming_batch_id, nest_part_id) DO UPDATE SET
                    part_attribute_id = excluded.part_attribute_id,
                    part_number = excluded.part_number,
                    part_revision = excluded.part_revision,
                    expected_quantity = excluded.expected_quantity,
                    is_complete = CASE
                        WHEN forming_batch_items.scanned_quantity >= excluded.expected_quantity THEN 1
                        ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    batch_id,
                    nest_part_id,
                    None,
                    str(reference_row["part_number"] or ""),
                    str(reference_row["part_revision"] or "-"),
                    expected_quantity,
                ),
            )

        if mark_started and batch_status != "completed":
            started_value = started_at or datetime.now().isoformat(timespec="seconds")
            connection.execute(
                "UPDATE forming_batches SET status = 'in_progress', started_at = COALESCE(started_at, ?) WHERE id = ?",
                (started_value, batch_id),
            )
            batch_status = "in_progress"
            started_at = started_value

        return batch_id, batch_status, created_at, started_at, run_number

    def _load_forming_batch_snapshot(self, dat_name: str, *, mark_started: bool = False) -> dict[str, Any] | None:
        with get_connection() as connection:
            create_schema(connection)
            rows = self._load_latest_tracker_rows_for_dat(connection, dat_name, requires_forming_only=True)
            if not rows:
                return None
            ready_rows = [
                row
                for row in rows
                if self._normalize_tracker_stage(str(row["stage"] or ""), requires_forming=True) == TRACKER_STAGE_CUT
            ]
            if not ready_rows:
                return None
            self._upsert_monitor_units(connection, rows, dat_name=dat_name)
            batch_id, batch_status, created_at, started_at, run_number = self._ensure_forming_batch(
                connection,
                dat_name,
                ready_rows,
                mark_started=mark_started,
            )
            item_rows = connection.execute(
                "SELECT nest_part_id, expected_quantity, scanned_quantity FROM forming_batch_items WHERE forming_batch_id = ?",
                (batch_id,),
            ).fetchall()
            connection.commit()

        items_by_nest = {
            int(row["nest_part_id"]): row
            for row in item_rows
        }
        expected_parts: list[dict[str, Any]] = []
        scanned_parts: list[dict[str, Any]] = []
        rows_by_nest: dict[int, list[Any]] = {}
        for row in ready_rows:
            rows_by_nest.setdefault(int(row["nest_part_id"]), []).append(row)

        for nest_part_id, grouped_rows in rows_by_nest.items():
            grouped_rows.sort(key=lambda item: (int(item["scan_sequence"] or 1), str(item["part_number"] or "")))
            item_row = items_by_nest.get(nest_part_id)
            expected_quantity = len(grouped_rows)
            scanned_quantity = int(item_row["scanned_quantity"] or 0) if item_row is not None else 0
            scanned_quantity = max(0, min(scanned_quantity, expected_quantity))

            for index, row in enumerate(grouped_rows, start=1):
                target_list = scanned_parts if index <= scanned_quantity else expected_parts
                target_list.append(
                    self._build_tracker_part_instance(
                        row,
                        forming_batch_id=batch_id,
                    )
                )

        return {
            "batch_id": batch_id,
            "dat_name": dat_name,
            "run_number": run_number,
            "status": batch_status,
            "queued_at": created_at,
            "loaded_at": started_at,
            "com_numbers": self._extract_com_numbers_from_rows(ready_rows),
            "expected_parts": expected_parts,
            "scanned_parts": scanned_parts,
        }

    def _apply_forming_snapshot_to_state(self, state: dict[str, Any], snapshot: dict[str, Any], *, mark_loaded: bool) -> None:
        dat_name = str(snapshot["dat_name"])
        existing_entry = next(
            (item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "") == dat_name),
            None,
        )
        active_entry = {
            "dat_name": dat_name,
            "run_number": int(snapshot.get("run_number") or 1),
            "loaded_at": snapshot.get("loaded_at") or datetime.now().isoformat(timespec="seconds"),
            "forming_batch_id": snapshot["batch_id"],
            "com_numbers": list(snapshot["com_numbers"]),
            "expected_parts": list(snapshot["expected_parts"]),
            "scanned_parts": list(snapshot["scanned_parts"]),
            "scan_edit_mode": bool(existing_entry.get("scan_edit_mode")) if existing_entry is not None else False,
        }
        active_index = next(
            (index for index, item in enumerate(state.get("formed_active_lists", [])) if str(item.get("dat_name") or "") == dat_name),
            None,
        )
        if mark_loaded:
            if active_index is None:
                state.setdefault("formed_active_lists", []).append(active_entry)
            else:
                state["formed_active_lists"][active_index] = active_entry
        elif active_index is not None:
            state["formed_active_lists"][active_index] = active_entry

    def _increment_forming_batch_item(self, forming_batch_id: int | None, part: dict[str, Any]) -> None:
        if forming_batch_id is None:
            return

        with get_connection() as connection:
            create_schema(connection)
            connection.execute(
                """
                UPDATE forming_batch_items
                SET scanned_quantity = CASE
                        WHEN scanned_quantity < expected_quantity THEN scanned_quantity + 1
                        ELSE scanned_quantity
                    END,
                    is_complete = CASE
                        WHEN scanned_quantity + 1 >= expected_quantity THEN 1
                        ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE forming_batch_id = ? AND nest_part_id = ?
                """,
                (int(forming_batch_id), int(part["nest_part_id"])),
            )
            connection.execute(
                """
                INSERT INTO scan_events (
                    event_type,
                    barcode_value,
                    part_number,
                    part_revision,
                    forming_batch_id,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "forming_scan",
                    str(part.get("part_number") or ""),
                    str(part.get("part_number") or ""),
                    str(part.get("part_revision") or ""),
                    int(forming_batch_id),
                    str(part.get("com_number") or ""),
                ),
            )
            self._touch_monitor_units(connection, [str(part.get("com_number") or "")])
            connection.commit()

    def load_expected_parts(self, dat_name: str) -> list[dict[str, Any]]:
        return list(self._load_flat_scan_snapshot(dat_name)["expected_parts"])

    def scan_field(self, field_name: str, value: str) -> dict[str, Any]:
        state = self.read()
        cleaned = value.strip()
        if not cleaned:
            raise UiStateError("Scanned value was blank.")

        if field_name == "machine_code":
            state["machine_code"] = cleaned
            if state.get("update_target") == "machine_code":
                state["update_target"] = ""
                if state.get("nest_data"):
                    state["active_field"] = "part_scan"
                    state["message"] = "Machine updated. Continue scanning parts"
                else:
                    state["active_field"] = "nest_data"
                    state["message"] = "Scan NEST DATA"
            else:
                state["active_field"] = "user_code"
                state["message"] = "Scan USER"
            state["message_level"] = "info"
        elif field_name == "user_code":
            state["user_code"] = cleaned
            if state.get("update_target") == "user_code":
                state["update_target"] = ""
                if state.get("nest_data"):
                    state["active_field"] = "part_scan"
                    state["message"] = "User updated. Continue scanning parts"
                else:
                    state["active_field"] = "nest_data"
                    state["message"] = "Scan NEST DATA"
            else:
                state["active_field"] = "location_code"
                state["message"] = "Scan LOCATION"
            state["message_level"] = "info"
        elif field_name == "location_code":
            state["location_code"] = cleaned
            if state.get("update_target") == "location_code":
                state["update_target"] = ""
                if state.get("nest_data"):
                    state["active_field"] = "part_scan"
                    state["message"] = "Location updated. Continue scanning parts"
                else:
                    state["active_field"] = "nest_data"
                    state["message"] = "Scan NEST DATA"
            else:
                state["active_field"] = "nest_data"
                state["message"] = "Scan NEST DATA"
            state["message_level"] = "info"
        elif field_name == "nest_data":
            with get_connection() as connection:
                create_schema(connection)
                rows = self._load_resolved_rows_for_dat(connection, cleaned)
                if not rows:
                    raise UiStateError(f"No resolved parts found for {cleaned}")
                repeat_info = self._load_repeat_scan_info(connection, cleaned, rows)

            if repeat_info["has_completed_run"]:
                state["nest_data"] = ""
                state["update_target"] = ""
                state["flat_scan_session_id"] = None
                state["flat_scan_status"] = ""
                state["current_run_number"] = 0
                state["expected_parts"] = []
                state["scanned_parts"] = []
                state["scan_edit_mode"] = False
                state["active_field"] = "nest_data"
                state["message"] = "Program is Already in System. Are you sure you want continue?"
                state["message_level"] = "warning"
                state["repeat_scan_pending"] = True
                state["pending_repeat_dat"] = cleaned
                state["pending_repeat_run_number"] = int(repeat_info["next_run_number"])
            else:
                snapshot = self._load_flat_scan_snapshot(cleaned)
                self._apply_flat_scan_snapshot(state, cleaned, snapshot)
        elif field_name == "part_scan":
            if not state.get("nest_data"):
                raise UiStateError("Scan nest data before scanning parts.")
            self._scan_part_into_state(state, cleaned)
        else:
            raise UiStateError(f"Unsupported field {field_name}")

        self.write(state)
        return state

    def confirm_repeat_scan(self) -> dict[str, Any]:
        state = self.read()
        if not state.get("repeat_scan_pending"):
            raise UiStateError("No repeat scan is waiting for confirmation.")

        dat_name = str(state.get("pending_repeat_dat") or "").strip()
        run_number = int(state.get("pending_repeat_run_number") or 0)
        if not dat_name or run_number <= 1:
            raise UiStateError("Repeat scan details were missing. Scan the DAT again.")

        snapshot = self._load_flat_scan_snapshot(
            dat_name,
            start_new_session=True,
            forced_run_number=run_number,
        )
        self._apply_flat_scan_snapshot(state, dat_name, snapshot)
        state["message"] = f"Run {run_number} started for {dat_name}. Start scanning parts."
        state["message_level"] = "warning"
        self.write(state)
        return state

    def cancel_repeat_scan(self) -> dict[str, Any]:
        state = self.read()
        self._clear_repeat_scan_prompt(state)
        state["active_field"] = "nest_data"
        state["message"] = "Repeat scan canceled. Scan NEST DATA."
        state["message_level"] = "info"
        self.write(state)
        return state

    def _load_latest_tracker_rows_for_dat(self, connection, dat_name: str, *, requires_forming_only: bool = False):
        latest_run_row = connection.execute(
            """
            SELECT COALESCE(MAX(run_number), 0) AS latest_run
            FROM part_tracker_items
            WHERE UPPER(TRIM(dat_name)) = UPPER(TRIM(?))
            """,
            (dat_name,),
        ).fetchone()
        latest_run = int(latest_run_row["latest_run"] or 0) if latest_run_row is not None else 0
        if latest_run <= 0:
            return []

        sql = """
            SELECT
                tracker_key,
                flat_scan_session_id,
                run_number,
                dat_name,
                nest_part_id,
                scan_sequence,
                part_number,
                part_revision,
                com_number,
                machine,
                user_code,
                location,
                requires_forming,
                stage
            FROM part_tracker_items
            WHERE UPPER(TRIM(dat_name)) = UPPER(TRIM(?))
              AND run_number = ?
        """
        params: list[Any] = [dat_name, latest_run]
        if requires_forming_only:
            sql += " AND requires_forming = 1"
        sql += " ORDER BY part_number, scan_sequence"
        return connection.execute(sql, tuple(params)).fetchall()

    def _load_formed_queue_preview(self, dat_name: str) -> dict[str, Any] | None:
        with get_connection() as connection:
            create_schema(connection)
            rows = self._load_latest_tracker_rows_for_dat(connection, dat_name, requires_forming_only=True)

        if not rows:
            return None

        queued_rows = [
            row
            for row in rows
            if self._normalize_tracker_stage(str(row["stage"] or ""), requires_forming=True)
            in {TRACKER_STAGE_PROG, TRACKER_STAGE_CUT}
        ]
        if not queued_rows:
            return None

        ready_count = sum(
            1
            for row in queued_rows
            if self._normalize_tracker_stage(str(row["stage"] or ""), requires_forming=True) == TRACKER_STAGE_CUT
        )
        return {
            "dat_name": str(queued_rows[0]["dat_name"] or dat_name),
            "run_number": int(queued_rows[0]["run_number"] or 1),
            "part_count": len(queued_rows),
            "ready_count": ready_count,
            "com_numbers": self._extract_com_numbers_from_rows(queued_rows),
        }

    @staticmethod
    def _remove_formed_queue_entry(state: dict[str, Any], dat_name: str) -> None:
        state["formed_queue"] = [
            item for item in state.get("formed_queue", []) if str(item.get("dat_name") or "") != dat_name
        ]

    @staticmethod
    def _upsert_formed_queue_entry(state: dict[str, Any], preview: dict[str, Any]) -> None:
        dat_name = str(preview.get("dat_name") or "")
        entry = {
            "dat_name": dat_name,
            "run_number": int(preview.get("run_number") or 1),
            "part_count": int(preview.get("part_count") or 0),
            "ready_count": int(preview.get("ready_count") or 0),
            "com_numbers": list(preview.get("com_numbers") or []),
        }
        queue_index = next(
            (index for index, item in enumerate(state.get("formed_queue", [])) if str(item.get("dat_name") or "") == dat_name),
            None,
        )
        if queue_index is None:
            state.setdefault("formed_queue", []).append(entry)
        else:
            state["formed_queue"][queue_index] = entry

    def _queue_formed_from_nest(self, state: dict[str, Any], dat_name: str) -> None:
        preview = self._load_formed_queue_preview(dat_name)
        if preview is None:
            self._remove_formed_queue_entry(state, dat_name)
            return
        if any(str(item.get("dat_name") or "") == dat_name for item in state.get("formed_active_lists", [])):
            return
        self._upsert_formed_queue_entry(state, preview)

    @staticmethod
    def _clear_formed_selection(state: dict[str, Any]) -> None:
        state["formed_selection_part"] = ""
        state["formed_selection_candidates"] = []

    @staticmethod
    def _sort_formed_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            candidates,
            key=lambda item: (
                0 if item.get("is_active") else 1,
                str((item.get("com_numbers") or [""])[0] or ""),
                str(item.get("dat_name") or ""),
            ),
        )

    @staticmethod
    def _build_formed_candidate(
        dat_name: str,
        run_number: int,
        com_numbers: list[str],
        part_count: int,
        ready_count: int,
        *,
        is_active: bool,
    ) -> dict[str, Any]:
        return {
            "dat_name": str(dat_name or ""),
            "run_number": int(run_number or 1),
            "com_numbers": [str(value or "").strip() for value in com_numbers if str(value or "").strip()],
            "part_count": int(part_count or 0),
            "ready_count": int(ready_count or 0),
            "is_active": bool(is_active),
            "selection_token": str(dat_name or ""),
        }

    def _build_formed_candidate_from_preview(self, preview: dict[str, Any], *, is_active: bool) -> dict[str, Any]:
        return self._build_formed_candidate(
            str(preview.get("dat_name") or ""),
            int(preview.get("run_number") or 1),
            list(preview.get("com_numbers") or []),
            int(preview.get("part_count") or 0),
            int(preview.get("ready_count") or 0),
            is_active=is_active,
        )

    def _build_formed_candidate_from_active(self, dat_list: dict[str, Any]) -> dict[str, Any]:
        expected_parts = list(dat_list.get("expected_parts") or [])
        scanned_parts = list(dat_list.get("scanned_parts") or [])
        return self._build_formed_candidate(
            str(dat_list.get("dat_name") or ""),
            int(dat_list.get("run_number") or 1),
            list(dat_list.get("com_numbers") or []),
            len(expected_parts) + len(scanned_parts),
            len(expected_parts),
            is_active=True,
        )

    def _scan_part_into_formed_list(self, state: dict[str, Any], dat_name: str, part_number: str) -> dict[str, Any]:
        for dat_list in state.get("formed_active_lists", []):
            if str(dat_list.get("dat_name") or "") != dat_name:
                continue
            expected = dat_list.get("expected_parts", [])
            idx = next((index for index, part in enumerate(expected) if str(part.get("part_number") or "") == part_number), None)
            if idx is None:
                raise UiStateError(f"Part {part_number} is not waiting on {dat_name}.")
            row = expected.pop(idx)
            dat_list.setdefault("scanned_parts", []).append(row)
            self._increment_forming_batch_item(dat_list.get("forming_batch_id"), row)
            return dat_list
        raise UiStateError(f"List {dat_name} is not loaded in this session.")

    def _load_queued_formed_part_candidates(self, state: dict[str, Any], part_number: str) -> list[dict[str, Any]]:
        active_dats = {
            str(item.get("dat_name") or "").strip().upper()
            for item in state.get("formed_active_lists", [])
            if str(item.get("dat_name") or "").strip()
        }

        with get_connection() as connection:
            create_schema(connection)
            rows = connection.execute(
                """
                WITH latest_runs AS (
                    SELECT UPPER(TRIM(dat_name)) AS dat_key, MAX(run_number) AS latest_run
                    FROM part_tracker_items
                    WHERE requires_forming = 1
                    GROUP BY UPPER(TRIM(dat_name))
                )
                SELECT DISTINCT pti.dat_name
                FROM part_tracker_items pti
                JOIN latest_runs lr
                  ON UPPER(TRIM(pti.dat_name)) = lr.dat_key
                 AND pti.run_number = lr.latest_run
                WHERE pti.requires_forming = 1
                  AND UPPER(TRIM(pti.part_number)) = UPPER(TRIM(?))
                  AND UPPER(TRIM(COALESCE(pti.stage, ''))) IN ('PROG', 'CUT')
                ORDER BY UPPER(TRIM(pti.dat_name))
                """,
                (part_number,),
            ).fetchall()

        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            dat_name = str(row["dat_name"] or "").strip().upper()
            if not dat_name or dat_name in active_dats or dat_name in seen:
                continue
            preview = self._load_formed_queue_preview(dat_name)
            if preview is None or int(preview.get("ready_count") or 0) <= 0:
                continue
            candidates.append(self._build_formed_candidate_from_preview(preview, is_active=False))
            seen.add(dat_name)
        return self._sort_formed_candidates(candidates)

    def _set_formed_selection(self, state: dict[str, Any], part_number: str, candidates: list[dict[str, Any]]) -> None:
        state["formed_selection_part"] = part_number
        state["formed_selection_candidates"] = self._sort_formed_candidates(candidates)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Part {part_number} is on multiple lists. Scan or click the correct DAT list below."
        state["formed_message_level"] = "error"

    def _select_formed_list(self, state: dict[str, Any], dat_token: str) -> dict[str, Any]:
        token = self._normalize_dat_token(dat_token)
        if not token:
            raise UiStateError("DAT token was blank.")

        pending_part = str(state.get("formed_selection_part") or "").strip()
        pending_candidates = {
            str(item.get("dat_name") or "").strip().upper()
            for item in state.get("formed_selection_candidates", [])
            if str(item.get("dat_name") or "").strip()
        }
        if pending_part and pending_candidates and token not in pending_candidates:
            raise UiStateError(f"Part {pending_part} is on multiple lists. Choose one of the highlighted DAT lists.")

        active_entry = next(
            (item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "").strip().upper() == token),
            None,
        )
        if active_entry is None:
            preview = self._load_formed_queue_preview(token)
            if preview is None:
                raise UiStateError(f"No formed parts are waiting for {token} in the latest run.")
            if int(preview.get("ready_count") or 0) <= 0:
                raise UiStateError(f"{token} is queued for forming, but cut stage is not complete yet.")

            snapshot = self._load_forming_batch_snapshot(token, mark_started=True)
            if snapshot is None:
                raise UiStateError(f"No formed parts are ready for {token}.")

            self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=True)
            self._remove_formed_queue_entry(state, token)
            active_entry = next(
                (item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "").strip().upper() == token),
                None,
            )

        if active_entry is None:
            raise UiStateError(f"Unable to load formed list for {token}.")

        if pending_part:
            self._scan_part_into_formed_list(state, str(active_entry.get("dat_name") or token), pending_part)
            self._clear_formed_selection(state)
            state["formed_active_field"] = "part_scan"
            state["formed_message"] = f"Selected {active_entry['dat_name']}. Accepted {pending_part}."
            state["formed_message_level"] = "success"
            return active_entry

        self._clear_formed_selection(state)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed run {int(active_entry.get('run_number') or 1)} for {active_entry['dat_name']}. Scan formed parts."
        state["formed_message_level"] = "info"
        return active_entry

    def _try_handle_formed_selection_scan(self, state: dict[str, Any], raw_value: str) -> bool:
        if not state.get("formed_selection_candidates"):
            return False
        token = self._normalize_dat_token(raw_value)
        if not token:
            return False
        candidates = {
            str(item.get("dat_name") or "").strip().upper()
            for item in state.get("formed_selection_candidates", [])
            if str(item.get("dat_name") or "").strip()
        }
        if token not in candidates:
            return False
        self._select_formed_list(state, token)
        return True

    @staticmethod
    def _normalize_dat_token(raw: str) -> str:
        token = raw.strip()
        if not token:
            return ""

        token = token.replace("\\", "/").split("/")[-1].strip()

        if ".DAT" in token.upper():
            match = re.search(r"([A-Za-z0-9._-]+\.DAT)", token, flags=re.IGNORECASE)
            if match:
                token = match.group(1)
        else:
            split_match = re.split(r"[|:;,]", token)
            if split_match:
                candidate = split_match[-1].strip()
                if candidate:
                    token = candidate
            if token and "." not in token:
                token = f"{token}.DAT"

        return token.upper()

    @staticmethod
    def _scan_looks_like_explicit_dat(raw: str) -> bool:
        token = str(raw or "").strip()
        if not token:
            return False
        return bool(re.search(r"\.dat\b", token, flags=re.IGNORECASE))

    def _has_formed_part_target(self, state: dict[str, Any], part_number: str) -> bool:
        cleaned = str(part_number or "").strip()
        if not cleaned:
            return False
        if any(
            str(part.get("part_number") or "") == cleaned
            for dat_list in state.get("formed_active_lists", [])
            for part in dat_list.get("expected_parts", [])
        ):
            return True
        return bool(self._load_queued_formed_part_candidates(state, cleaned))

    def _is_known_formed_dat_token(self, state: dict[str, Any], raw_value: str) -> bool:
        token = self._normalize_dat_token(raw_value)
        if not token:
            return False
        if any(
            str(item.get("dat_name") or "").strip().upper() == token
            for item in state.get("formed_active_lists", [])
        ):
            return True
        if any(
            str(item.get("dat_name") or "").strip().upper() == token
            for item in state.get("formed_selection_candidates", [])
        ):
            return True
        return self._load_formed_queue_preview(token) is not None

    def _should_route_formed_scan_as_dat(self, state: dict[str, Any], raw_value: str) -> bool:
        if self._scan_looks_like_explicit_dat(raw_value):
            return True
        if state.get("formed_selection_candidates") and self._is_known_formed_dat_token(state, raw_value):
            return True
        if self._has_formed_part_target(state, raw_value):
            return False
        return self._is_known_formed_dat_token(state, raw_value)

    def _scan_part_into_state(self, state: dict[str, Any], part_number: str) -> None:
        target_index = next(
            (index for index, part in enumerate(state["expected_parts"]) if part["part_number"] == part_number),
            None,
        )
        target = state["expected_parts"][target_index] if target_index is not None else None
        if target is None:
            raise UiStateError(f"Part {part_number} is not expected or is already complete.")

        state["expected_parts"].pop(target_index)
        self._mark_part_scanned(state, target)
        self._increment_flat_scan_item(state.get("flat_scan_session_id"), target)

        remaining = len(state["expected_parts"])
        if remaining == 0:
            state["message"] = "All parts scanned. Complete the batch when ready or scan the next DAT."
            state["active_field"] = "machine_code"
            state["message_level"] = "success"
        else:
            state["message"] = f"Accepted {part_number}. {remaining} part scans remaining."
            state["active_field"] = "part_scan"
            state["message_level"] = "success"

    def _mark_part_scanned(self, state: dict[str, Any], part: dict[str, Any]) -> None:
        part["machine"] = str(state.get("machine_code") or "")
        part["user_code"] = str(state.get("user_code") or "")
        part["location"] = str(part.get("location") or state.get("location_code") or "")
        state["scanned_parts"].append(part)

    def auto_fill_current_batch(self) -> tuple[str, int]:
        state = self.read()
        dat_name = str(state.get("nest_data") or "").strip().upper()
        if not dat_name:
            raise UiStateError("Scan NEST DATA before using scanner auto mode.")

        pending_parts = [dict(part) for part in state.get("expected_parts", [])]
        if not pending_parts:
            return dat_name, 0

        state["expected_parts"] = []
        for part in pending_parts:
            self._mark_part_scanned(state, part)

        self._increment_flat_scan_items(state.get("flat_scan_session_id"), pending_parts)
        state["scan_edit_mode"] = False
        state["active_field"] = "machine_code"
        state["message"] = (
            f"Auto complete moved {len(pending_parts)} parts to Scanned. Click Complete when ready or scan the next DAT."
        )
        state["message_level"] = "success"
        self.write(state)
        return dat_name, len(pending_parts)

    def invalidate_scan(self, message: str) -> dict[str, Any]:
        state = self.read()
        state["message"] = f"INVALID SCAN — {message}"
        state["message_level"] = "error"
        self.write(state)
        return state

    def formed_context(self) -> dict[str, Any]:
        state = self.read()
        known_dats = {
            str(item.get("dat_name") or "").strip()
            for item in list(state.get("formed_queue", [])) + list(state.get("formed_active_lists", []))
            if str(item.get("dat_name") or "").strip()
        }
        changed = False
        for dat_name in sorted(known_dats):
            active_dat = next(
                (item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "").strip() == dat_name),
                None,
            )
            if active_dat is not None:
                snapshot = self._load_forming_batch_snapshot(dat_name, mark_started=False)
                if snapshot is None:
                    state["formed_active_lists"] = [
                        item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "") != dat_name
                    ]
                else:
                    self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=True)
                self._remove_formed_queue_entry(state, dat_name)
                changed = True
                continue

            preview = self._load_formed_queue_preview(dat_name)
            if preview is None:
                before_count = len(state.get("formed_queue", []))
                self._remove_formed_queue_entry(state, dat_name)
                changed = changed or len(state.get("formed_queue", [])) != before_count
                continue

            self._upsert_formed_queue_entry(state, preview)
            changed = True
        if changed:
            self.write(state)

        lists: list[dict[str, Any]] = []
        for dat_list in state["formed_active_lists"]:
            payload = dict(dat_list)
            payload["can_complete"] = not payload.get("expected_parts") and bool(payload.get("scanned_parts"))
            payload["can_force_complete"] = bool(payload.get("expected_parts") or payload.get("scanned_parts"))
            payload["can_edit_scanned"] = bool(payload.get("scanned_parts"))
            lists.append(payload)

        selection_candidates: list[dict[str, Any]] = []
        selection_part = str(state.get("formed_selection_part") or "").strip()
        if selection_part and state.get("formed_selection_candidates"):
            for candidate in state.get("formed_selection_candidates", []):
                dat_name = str(candidate.get("dat_name") or "").strip()
                if not dat_name:
                    continue
                active_entry = next(
                    (item for item in state.get("formed_active_lists", []) if str(item.get("dat_name") or "").strip() == dat_name),
                    None,
                )
                if active_entry is not None:
                    selection_candidates.append(self._build_formed_candidate_from_active(active_entry))
                    continue
                preview = self._load_formed_queue_preview(dat_name)
                if preview is None or int(preview.get("ready_count") or 0) <= 0:
                    continue
                selection_candidates.append(self._build_formed_candidate_from_preview(preview, is_active=False))
            selection_candidates = self._sort_formed_candidates(selection_candidates)
            if selection_candidates != list(state.get("formed_selection_candidates", [])):
                state["formed_selection_candidates"] = selection_candidates
                changed = True
            if not selection_candidates:
                self._clear_formed_selection(state)
                selection_part = ""
                changed = True

        if changed:
            self.write(state)

        queue = self._sort_formed_candidates(
            [self._build_formed_candidate_from_preview(item, is_active=False) for item in state.get("formed_queue", [])]
        )
        return {
            "queue": queue,
            "lists": lists,
            "active_field": state["formed_active_field"],
            "message": state["formed_message"],
            "message_level": state["formed_message_level"],
            "selection_conflict": (
                {
                    "part_number": selection_part,
                    "candidates": selection_candidates,
                }
                if selection_part and selection_candidates
                else None
            ),
        }

    def formed_scan_value(self, raw_value: str) -> dict[str, Any]:
        state = self.read()
        cleaned = str(raw_value or "").strip()
        if not cleaned:
            raise UiStateError("Scanned value was blank.")

        if self._should_route_formed_scan_as_dat(state, cleaned):
            self._select_formed_list(state, cleaned)
            self.write(state)
            return state

        return self.formed_scan_part(cleaned)

    def formed_scan_dat(self, dat_token: str) -> dict[str, Any]:
        state = self.read()
        self._select_formed_list(state, dat_token)
        self.write(state)
        return state

    def _try_load_queued_dat_into_formed(self, state: dict[str, Any], dat_token: str) -> tuple[bool, str | None]:
        token = self._normalize_dat_token(dat_token)
        if not token:
            return False, None

        preview = self._load_formed_queue_preview(token)
        if preview is None or int(preview.get("ready_count") or 0) <= 0:
            return False, token

        snapshot = self._load_forming_batch_snapshot(token, mark_started=True)
        if snapshot is None:
            return False, token

        self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=True)
        self._remove_formed_queue_entry(state, token)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed run {snapshot['run_number']} for {snapshot['dat_name']}. Scan formed parts."
        state["formed_message_level"] = "info"
        return True, token

    @staticmethod
    def _extract_com_numbers(parts: list[dict[str, Any]]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for part in parts:
            raw = str(part.get("com_number") or "").strip()
            if not raw or raw in seen:
                continue
            seen.add(raw)
            ordered.append(raw)
        return ordered

    @staticmethod
    def _should_ignore_csv_estimate_path(path_value: str | None) -> bool:
        raw = str(path_value or "").strip()
        if not raw:
            return False
        path = Path(raw)
        if is_ignored_source_path(path):
            return True
        return any(str(part).casefold() == "old" for part in path.parts[:-1])

    def _load_csv_estimate_total(self, connection, com_number: str) -> int:
        rows = connection.execute(
            """
            SELECT
                jp.source_file_path,
                COALESCE(jp.quantity, 0) AS quantity,
                COALESCE(jp.nested_on, '') AS nested_on,
                COALESCE(pa.collection_cart, '') AS collection_cart
            FROM job_parts jp
            JOIN job_folders jf ON jf.id = jp.job_folder_id
            LEFT JOIN part_attributes pa
              ON pa.source_file_path = jp.source_file_path
             AND COALESCE(pa.com_number, '') = COALESCE(jf.com_number, '')
             AND pa.part_number = jp.part_number
             AND COALESCE(pa.rev_level, '') = COALESCE(jp.revision, '')
             AND COALESCE(pa.build_date, '') = COALESCE(jp.build_date_code, '')
            WHERE jf.com_number = ?
              AND jp.source_type = 'nest_comparison'
            """,
            (com_number,),
        ).fetchall()

        total = 0
        for row in rows:
            if self._should_ignore_csv_estimate_path(row["source_file_path"]):
                continue
            nested_on = str(row["nested_on"] or "").strip().casefold()
            if nested_on == "not nested":
                continue
            collection_cart = str(row["collection_cart"] or "").strip().casefold()
            if nested_on == "eclipse" and collection_cart == "walls_channels":
                continue
            total += int(row["quantity"] or 0)
        return total

    def _load_monitor_totals(self, connection, com_number: str) -> dict[str, int]:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(quantity_nested), 0) AS total_parts,
                COALESCE(SUM(CASE WHEN requires_forming THEN quantity_nested ELSE 0 END), 0) AS total_forming_parts,
                COUNT(DISTINCT barcode_filename) AS dat_count,
                COUNT(DISTINCT part_number || '|' || COALESCE(part_revision, '')) AS distinct_parts
            FROM resolved_nest_parts
            WHERE com_number = ?
            """,
            (com_number,),
        ).fetchone()
        csv_total_parts = self._load_csv_estimate_total(connection, com_number)

        return {
            "total_parts": int(row["total_parts"] or 0) if row is not None else 0,
            "total_forming_parts": int(row["total_forming_parts"] or 0) if row is not None else 0,
            "dat_count": int(row["dat_count"] or 0) if row is not None else 0,
            "distinct_parts": int(row["distinct_parts"] or 0) if row is not None else 0,
            "csv_total_parts": csv_total_parts,
        }

    def _load_monitor_progress(self, connection, com_number: str) -> dict[str, int]:
        row = connection.execute(
            """
            SELECT
                COALESCE((
                    SELECT SUM(scanned_quantity)
                    FROM (
                        SELECT MAX(fi.scanned_quantity) AS scanned_quantity
                        FROM flat_scan_items fi
                        JOIN resolved_nest_parts r ON r.nest_part_id = fi.nest_part_id
                        WHERE r.com_number = ?
                        GROUP BY fi.nest_part_id
                    )
                ), 0) AS flat_done,
                COALESCE((
                    SELECT SUM(scanned_quantity)
                    FROM (
                        SELECT MAX(fbi.scanned_quantity) AS scanned_quantity
                        FROM forming_batch_items fbi
                        JOIN resolved_nest_parts r ON r.nest_part_id = fbi.nest_part_id
                        WHERE r.com_number = ?
                        GROUP BY fbi.nest_part_id
                    )
                ), 0) AS forming_done
            """,
            (com_number, com_number),
        ).fetchone()
        return {
            "flat_done": int(row["flat_done"] or 0) if row is not None else 0,
            "forming_done": int(row["forming_done"] or 0) if row is not None else 0,
        }

    @staticmethod
    def _progress_percent(completed: int, total: int) -> int:
        if total <= 0:
            return 100
        return max(0, min(100, int(round((completed / total) * 100))))

    def monitor_context(self) -> dict[str, Any]:
        with get_connection() as connection:
            create_schema(connection)
            monitor_rows = connection.execute(
                """
                SELECT
                    mu.id,
                    mu.com_number,
                    mu.status,
                    mu.started_at,
                    mu.last_activity_at,
                    GROUP_CONCAT(mus.barcode_filename, '||') AS source_dats
                FROM monitor_units mu
                LEFT JOIN monitor_unit_sources mus ON mus.monitor_unit_id = mu.id
                GROUP BY mu.id, mu.com_number, mu.status, mu.started_at, mu.last_activity_at
                ORDER BY mu.started_at DESC, mu.com_number
                """
            ).fetchall()

            units: list[dict[str, Any]] = []
            for item in monitor_rows:
                com_number = str(item["com_number"] or "").strip()
                if not com_number:
                    continue
                totals = self._load_monitor_totals(connection, com_number)
                progress = self._load_monitor_progress(connection, com_number)

                total_parts = totals["total_parts"] or progress["flat_done"]
                total_forming = totals["total_forming_parts"] or progress["forming_done"]
                flat_done = min(progress["flat_done"], total_parts or progress["flat_done"])
                forming_done = min(progress["forming_done"], total_forming or progress["forming_done"])
                remaining_parts = max(0, total_parts - flat_done)
                waiting_forming = max(0, total_forming - forming_done)
                flat_progress = self._progress_percent(flat_done, total_parts)
                forming_progress = self._progress_percent(forming_done, total_forming) if total_forming > 0 else 100
                is_complete = remaining_parts == 0 and waiting_forming == 0
                computed_status = "complete" if is_complete else "in_progress"
                if str(item["status"] or "") != computed_status:
                    connection.execute(
                        "UPDATE monitor_units SET status = ? WHERE id = ?",
                        (computed_status, int(item["id"])),
                    )

                source_dats_raw = str(item["source_dats"] or "")
                source_dats = [value for value in source_dats_raw.split("||") if value]
                units.append(
                    {
                        "com_number": com_number,
                        "started_at": str(item["started_at"] or ""),
                        "source_dats": source_dats,
                        "dat_count": totals["dat_count"],
                        "distinct_parts": totals["distinct_parts"],
                        "csv_total_parts": totals["csv_total_parts"],
                        "total_parts": total_parts,
                        "flat_done": flat_done,
                        "remaining_parts": remaining_parts,
                        "flat_progress": flat_progress,
                        "total_forming": total_forming,
                        "forming_done": forming_done,
                        "waiting_forming": waiting_forming,
                        "forming_progress": forming_progress,
                        "status": "Complete" if is_complete else "In Progress",
                        "is_complete": is_complete,
                    }
                )

            connection.commit()

        units.sort(key=lambda unit: (unit["is_complete"], unit["started_at"], unit["com_number"]))

        active_units = [unit for unit in units if not unit["is_complete"]]
        summary = {
            "units_in_progress": len(active_units),
            "parts_in_progress": sum(unit["remaining_parts"] for unit in active_units),
            "parts_waiting_forming": sum(unit["waiting_forming"] for unit in active_units),
            "parts_formed": sum(unit["forming_done"] for unit in units),
        }

        return {
            "summary": summary,
            "units": units,
        }

    def formed_scan_part(self, part_number: str) -> dict[str, Any]:
        state = self.read()
        cleaned = part_number.strip()
        if not cleaned:
            raise UiStateError("Scanned value was blank.")

        if self._try_handle_formed_selection_scan(state, cleaned):
            self.write(state)
            return state

        if state.get("formed_selection_candidates"):
            pending_part = str(state.get("formed_selection_part") or "").strip()
            raise UiStateError(f"Part {pending_part} is on multiple lists. Scan or click one of the highlighted DAT lists.")

        active_candidates = self._sort_formed_candidates(
            [
                self._build_formed_candidate_from_active(dat_list)
                for dat_list in state.get("formed_active_lists", [])
                if any(str(part.get("part_number") or "") == cleaned for part in dat_list.get("expected_parts", []))
            ]
        )
        queued_candidates = self._load_queued_formed_part_candidates(state, cleaned)
        all_candidates = self._sort_formed_candidates(active_candidates + queued_candidates)

        if len(all_candidates) > 1:
            self._set_formed_selection(state, cleaned, all_candidates)
            self.write(state)
            return state

        if len(all_candidates) == 1:
            candidate = all_candidates[0]
            if candidate.get("is_active"):
                self._scan_part_into_formed_list(state, str(candidate.get("dat_name") or ""), cleaned)
                self._clear_formed_selection(state)
                state["formed_active_field"] = "part_scan"
                state["formed_message"] = f"Accepted {cleaned} into {candidate['dat_name']}."
                state["formed_message_level"] = "success"
            else:
                state["formed_selection_part"] = cleaned
                state["formed_selection_candidates"] = [candidate]
                self._select_formed_list(state, str(candidate.get("dat_name") or ""))
            self.write(state)
            return state

        matched = False
        matched_dat = None
        for dat_list in state["formed_active_lists"]:
            expected = dat_list["expected_parts"]
            idx = next((i for i, p in enumerate(expected) if p["part_number"] == cleaned), None)
            if idx is not None:
                row = expected.pop(idx)
                dat_list["scanned_parts"].append(row)
                self._increment_forming_batch_item(dat_list.get("forming_batch_id"), row)
                matched = True
                matched_dat = dat_list["dat_name"]
                break

        if not matched:
            loaded_dat, _ = self._try_load_queued_dat_into_formed(state, cleaned)
            if loaded_dat:
                self._clear_formed_selection(state)
                self.write(state)
                return state
            raise UiStateError(f"Part {cleaned} is not expected in any active formed list.")

        self._clear_formed_selection(state)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Accepted {cleaned} into {matched_dat}."
        state["formed_message_level"] = "success"
        self.write(state)
        return state

    def invalidate_formed_scan(self, message: str) -> dict[str, Any]:
        state = self.read()
        state["formed_message"] = f"INVALID SCAN — {message}"
        state["formed_message_level"] = "error"
        self.write(state)
        return state

    @staticmethod
    def _find_formed_list(state: dict[str, Any], batch_id: int) -> tuple[int, dict[str, Any]]:
        for index, dat_list in enumerate(state.get("formed_active_lists", [])):
            if int(dat_list.get("forming_batch_id") or 0) == int(batch_id):
                return index, dat_list
        raise UiStateError("Formed batch was not found in this session.")

    def start_formed_scan_edit(self, batch_id: int | str) -> dict[str, Any]:
        state = self.read()
        index, dat_list = self._find_formed_list(state, int(batch_id))
        if not dat_list.get("scanned_parts"):
            raise UiStateError("Scan at least one formed part before editing the scanned list.")
        state["formed_active_lists"][index]["scan_edit_mode"] = True
        state["formed_message"] = f"Edit formed scanned parts for {dat_list['dat_name']}, then click Done."
        state["formed_message_level"] = "info"
        self.write(state)
        return state

    def save_formed_scan_edits(self, form) -> dict[str, Any]:
        state = self.read()
        batch_id = int(str(form.get("batch_id", "0") or "0"))
        index, dat_list = self._find_formed_list(state, batch_id)
        if not dat_list.get("scan_edit_mode"):
            return state

        updated_parts: list[dict[str, Any]] = []
        for part_index, raw_part in enumerate(dat_list.get("scanned_parts", [])):
            part = dict(raw_part)
            part_number = str(form.get(f"formed_scanned_{part_index}_part_number", part.get("part_number", "")) or "").strip()
            com_number = str(form.get(f"formed_scanned_{part_index}_com_number", part.get("com_number", "")) or "").strip()
            location = str(form.get(f"formed_scanned_{part_index}_location", part.get("location", "")) or "").strip()
            if not part_number:
                raise UiStateError("Part number cannot be blank in formed edit mode.")
            part["part_number"] = part_number
            part["com_number"] = com_number
            part["location"] = location
            updated_parts.append(part)

        state["formed_active_lists"][index]["scanned_parts"] = updated_parts
        state["formed_active_lists"][index]["scan_edit_mode"] = False
        state["formed_message"] = f"Formed edits saved for {dat_list['dat_name']}. Click Complete or Force Complete to submit them."
        state["formed_message_level"] = "success"
        self.write(state)
        return state

    def formed_complete_current_batch(self, batch_id: int | str) -> int:
        state = self.read()
        index, dat_list = self._find_formed_list(state, int(batch_id))
        if dat_list["expected_parts"]:
            raise UiStateError("Cannot complete formed batch yet. There are still expected parts remaining.")
        if not dat_list["scanned_parts"]:
            raise UiStateError("No formed scanned parts to complete.")

        count = self._apply_tracker_stage(
            state,
            list(dat_list["scanned_parts"]),
            TRACKER_STAGE_FORMED,
            event_type="formed_complete",
            scanner_name="formed",
            notes="Formed scanner complete submitted Formed stage.",
        )

        now = datetime.now().isoformat(timespec="seconds")
        with get_connection() as connection:
            create_schema(connection)
            connection.execute(
                """
                UPDATE forming_batch_items
                SET is_complete = CASE
                        WHEN scanned_quantity >= expected_quantity THEN 1
                        ELSE 0
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE forming_batch_id = ?
                """,
                (int(dat_list["forming_batch_id"]),),
            )
            connection.execute(
                "UPDATE forming_batches SET status = 'completed', completed_at = COALESCE(completed_at, ?) WHERE id = ?",
                (now, int(dat_list["forming_batch_id"])),
            )
            self._touch_monitor_units(connection, self._extract_com_numbers(dat_list["scanned_parts"]))
            connection.commit()

        state["formed_active_lists"].pop(index)
        self._remove_formed_queue_entry(state, str(dat_list.get("dat_name") or ""))
        state["formed_active_field"] = "part_scan" if state.get("formed_active_lists") else "dat_token"
        state["formed_message"] = f"Formed batch completed. Updated {count} parts to Formed."
        state["formed_message_level"] = "success"
        self.write(state)
        return count

    def formed_force_complete_current_batch(self, batch_id: int | str) -> tuple[int, int]:
        state = self.read()
        index, dat_list = self._find_formed_list(state, int(batch_id))
        scanned_parts = list(dat_list.get("scanned_parts", []))
        missing_parts = [dict(part) for part in dat_list.get("expected_parts", [])]
        if not scanned_parts and not missing_parts:
            raise UiStateError("No formed batch loaded to force complete.")

        scanned_count = self._apply_tracker_stage(
            state,
            scanned_parts,
            TRACKER_STAGE_FORMED,
            event_type="formed_force_complete",
            scanner_name="formed",
            notes="Formed scanner force complete submitted Formed stage.",
        )
        missing_count = self._apply_tracker_stage(
            state,
            missing_parts,
            TRACKER_STAGE_MISSING,
            event_type="formed_force_missing",
            scanner_name="formed",
            notes="Formed scanner force complete marked part Missing.",
        )

        now = datetime.now().isoformat(timespec="seconds")
        with get_connection() as connection:
            create_schema(connection)
            connection.execute(
                "UPDATE forming_batches SET status = 'completed', completed_at = COALESCE(completed_at, ?) WHERE id = ?",
                (now, int(dat_list["forming_batch_id"])),
            )
            self._touch_monitor_units(connection, self._extract_com_numbers(scanned_parts + missing_parts))
            connection.commit()

        state["formed_active_lists"].pop(index)
        self._remove_formed_queue_entry(state, str(dat_list.get("dat_name") or ""))
        state["formed_active_field"] = "part_scan" if state.get("formed_active_lists") else "dat_token"
        state["formed_message"] = f"Formed force complete updated {scanned_count} parts to Formed and {missing_count} to Missing."
        state["formed_message_level"] = "success"
        self.write(state)
        return scanned_count, missing_count

    def complete_current_batch(self) -> int:
        state = self.read()
        if state["expected_parts"]:
            raise UiStateError("Cannot complete yet. There are still expected parts remaining.")
        if not state["scanned_parts"]:
            raise UiStateError("No scanned parts to complete.")

        now = datetime.now().isoformat(timespec="seconds")
        count = self._apply_tracker_stage(
            state,
            list(state["scanned_parts"]),
            TRACKER_STAGE_CUT,
            event_type="main_complete",
            scanner_name="main",
            notes="Main scanner complete submitted Cut stage.",
        )

        flat_scan_session_id = state.get("flat_scan_session_id")
        if flat_scan_session_id is not None:
            with get_connection() as connection:
                create_schema(connection)
                connection.execute(
                    """
                    UPDATE flat_scan_items
                    SET is_complete = CASE
                            WHEN scanned_quantity >= expected_quantity THEN 1
                            ELSE 0
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE flat_scan_session_id = ?
                    """,
                    (int(flat_scan_session_id),),
                )
                connection.execute(
                    "UPDATE flat_scan_sessions SET status = 'completed', completed_at = COALESCE(completed_at, ?) WHERE id = ?",
                    (now, int(flat_scan_session_id)),
                )
                self._touch_monitor_units(connection, self._extract_com_numbers(state["scanned_parts"]))
                connection.commit()

        self._reset_after_batch_submission(state, f"Batch completed. Updated {count} parts to Cut. Scan NEST DATA.")
        return count

    def force_complete_current_batch(self) -> tuple[int, int]:
        state = self.read()
        if not state.get("nest_data"):
            raise UiStateError("Scan NEST DATA before forcing completion.")

        scanned_parts = list(state.get("scanned_parts", []))
        missing_parts = [dict(part) for part in state.get("expected_parts", [])]
        if not scanned_parts and not missing_parts:
            raise UiStateError("No active batch loaded to force complete.")

        scanned_count = self._apply_tracker_stage(
            state,
            scanned_parts,
            TRACKER_STAGE_CUT,
            event_type="main_force_complete",
            scanner_name="main",
            notes="Main scanner force complete submitted Cut stage.",
        )
        missing_count = self._apply_tracker_stage(
            state,
            missing_parts,
            TRACKER_STAGE_MISSING,
            event_type="main_force_missing",
            scanner_name="main",
            notes="Main scanner force complete marked part Missing.",
        )

        flat_scan_session_id = state.get("flat_scan_session_id")
        if flat_scan_session_id is not None:
            now = datetime.now().isoformat(timespec="seconds")
            with get_connection() as connection:
                create_schema(connection)
                connection.execute(
                    "UPDATE flat_scan_sessions SET status = 'completed', completed_at = COALESCE(completed_at, ?) WHERE id = ?",
                    (now, int(flat_scan_session_id)),
                )
                self._touch_monitor_units(connection, self._extract_com_numbers(scanned_parts + missing_parts))
                connection.commit()

        self._reset_after_batch_submission(
            state,
            f"Force complete sent {scanned_count} parts to Cut and {missing_count} parts to Missing. Scan NEST DATA.",
        )
        return scanned_count, missing_count

    def clear_session_data(self) -> dict[str, Any]:
        state = self._default_state()
        self.write(state)
        return state

    def clear_development_progress(self) -> None:
        with get_connection() as connection:
            create_schema(connection)
            connection.executescript(
                """
                DELETE FROM part_tracker_items;
                DELETE FROM part_tracker_history;
                DELETE FROM scan_events;
                DELETE FROM flat_scan_items;
                DELETE FROM flat_scan_sessions;
                DELETE FROM forming_batch_items;
                DELETE FROM forming_batches;
                DELETE FROM monitor_unit_sources;
                DELETE FROM monitor_units;
                DELETE FROM sqlite_sequence WHERE name IN (
                    'scan_events',
                    'part_tracker_items',
                    'part_tracker_history',
                    'flat_scan_items',
                    'flat_scan_sessions',
                    'forming_batch_items',
                    'forming_batches',
                    'monitor_unit_sources',
                    'monitor_units'
                );
                """
            )
            connection.commit()

        UiStateStore.clear_all_persisted_state()

    def clear_runtime_data(self) -> dict[str, Any]:
        state = self._default_state()
        self.write(state)
        with get_connection() as connection:
            create_schema(connection)
            connection.execute("DELETE FROM part_tracker_items")
            connection.execute("DELETE FROM part_tracker_history")
            connection.commit()
        self._write_completed([])
        self._write_missed([])
        return state

    def get_completed_list(self, search_query: str = "") -> list[dict[str, Any]]:
        query = str(search_query or "").strip()
        sql = """
            SELECT
                tracker_key,
                run_number,
                (
                    SELECT COALESCE(MAX(latest.run_number), 1)
                    FROM part_tracker_items latest
                    WHERE UPPER(TRIM(latest.dat_name)) = UPPER(TRIM(part_tracker_items.dat_name))
                ) AS latest_run_number,
                dat_name,
                part_number,
                part_revision,
                com_number,
                machine,
                user_code,
                location,
                requires_forming,
                stage,
                stage_updated_at,
                created_at,
                updated_at
            FROM part_tracker_items
        """
        params: tuple[Any, ...] = ()
        if query:
            pattern = f"%{query.replace('%', r'\%').replace('_', r'\_')}%"
            sql += """
                WHERE part_number LIKE ? ESCAPE '\\' COLLATE NOCASE
                   OR COALESCE(com_number, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
                   OR COALESCE(dat_name, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
                   OR COALESCE(location, '') LIKE ? ESCAPE '\\' COLLATE NOCASE
            """
            params = (pattern, pattern, pattern, pattern)
        sql += " ORDER BY stage_updated_at DESC, updated_at DESC, dat_name DESC, part_number, scan_sequence"

        with get_connection() as connection:
            create_schema(connection)
            rows = connection.execute(sql, params).fetchall()

        return [self._tracker_row_payload(row) for row in rows]

    def get_part_history(self, tracker_key: str) -> dict[str, Any]:
        cleaned = str(tracker_key or "").strip()
        if not cleaned:
            raise UiStateError("Part history requires a tracker row.")

        with get_connection() as connection:
            create_schema(connection)
            current_row = connection.execute(
                """
                SELECT
                    tracker_key,
                    run_number,
                    run_number AS latest_run_number,
                    dat_name,
                    nest_part_id,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    stage_updated_at,
                    created_at,
                    updated_at
                FROM part_tracker_items
                WHERE tracker_key = ?
                """,
                (cleaned,),
            ).fetchone()

            if current_row is None:
                history_seed = connection.execute(
                    """
                    SELECT dat_name, nest_part_id, scan_sequence
                    FROM part_tracker_history
                    WHERE tracker_key = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (cleaned,),
                ).fetchone()
                if history_seed is None:
                    raise UiStateError("Part history was not found.")
                dat_name = str(history_seed["dat_name"] or "")
                nest_part_id = int(history_seed["nest_part_id"]) if history_seed["nest_part_id"] is not None else None
                sequence = int(history_seed["scan_sequence"] or 1)
            else:
                dat_name = str(current_row["dat_name"] or "")
                nest_part_id = int(current_row["nest_part_id"]) if current_row["nest_part_id"] is not None else None
                sequence = int(current_row["scan_sequence"] or 1)

            history_rows = connection.execute(
                """
                SELECT
                    tracker_key,
                    event_type,
                    scanner_name,
                    dat_name,
                    run_number,
                    nest_part_id,
                    scan_sequence,
                    part_number,
                    part_revision,
                    com_number,
                    machine,
                    user_code,
                    location,
                    requires_forming,
                    stage,
                    recorded_at,
                    notes
                FROM part_tracker_history
                WHERE UPPER(TRIM(dat_name)) = UPPER(TRIM(?))
                  AND COALESCE(nest_part_id, -1) = COALESCE(?, -1)
                  AND COALESCE(scan_sequence, 1) = ?
                ORDER BY recorded_at DESC, id DESC
                """,
                (dat_name, nest_part_id, sequence),
            ).fetchall()

        current_payload = self._tracker_row_payload(current_row) if current_row is not None else None
        history_payload = [self._history_row_payload(row) for row in history_rows]
        return {
            "current_row": current_payload,
            "history_rows": history_payload,
            "dat_name": dat_name,
            "scan_sequence": sequence,
        }

    def get_missed_list(self) -> list[dict[str, Any]]:
        return self._read_missed()

    def clear_completed_list(self) -> int:
        with get_connection() as connection:
            create_schema(connection)
            row = connection.execute("SELECT COUNT(*) AS count FROM part_tracker_items").fetchone()
            removed = int(row["count"] or 0) if row is not None else 0
            connection.execute("DELETE FROM part_tracker_items")
            connection.execute("DELETE FROM part_tracker_history")
            connection.commit()
        self._write_completed([])
        return removed

    def clear_missed_list(self) -> int:
        existing = self._read_missed()
        removed = len(existing)
        self._write_missed([])
        return removed

    @staticmethod
    def summary(state: dict[str, Any]) -> dict[str, int]:
        expected_total = len(state["expected_parts"]) + len(state["scanned_parts"])
        scanned_total = len(state["scanned_parts"])
        remaining_total = max(0, expected_total - scanned_total)
        return {
            "expected_total": expected_total,
            "scanned_total": scanned_total,
            "remaining_total": remaining_total,
        }

    @staticmethod
    def expected_remaining_list(state: dict[str, Any]) -> list[dict[str, Any]]:
        return list(state["expected_parts"])

    @staticmethod
    def scanned_counts(state: dict[str, Any]) -> list[dict[str, Any]]:
        return list(state["scanned_parts"])
