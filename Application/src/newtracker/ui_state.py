from __future__ import annotations

import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import DATA_DIR, get_connection
from .persistence import atomic_write_json, read_json_file
from .schema import create_schema

LEGACY_UI_STATE_PATH = DATA_DIR / "ui_scan_state.json"
LEGACY_COMPLETED_LIST_PATH = DATA_DIR / "completed_scan_list.json"
LEGACY_MISSED_LIST_PATH = DATA_DIR / "missed_scan_list.json"
UI_SESSION_DIR = DATA_DIR / "ui_sessions"
LEGACY_MIGRATION_MARKER = UI_SESSION_DIR / ".legacy-migrated.json"
_UI_STATE_LOCK = threading.RLock()

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
            "active_field": "machine_code",
            "expected_parts": [],
            "scanned_parts": [],
            "message": "Enter or scan MACHINE",
            "message_level": "info",
            "formed_queue": [],
            "formed_active_lists": [],
            "formed_active_field": "dat_token",
            "formed_message": "Scan DAT token to load formed list",
            "formed_message_level": "info",
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

    def _ensure_flat_scan_session(self, connection, dat_name: str, rows: list[Any]) -> tuple[int, str]:
        if not rows:
            raise UiStateError(f"No resolved parts found for {dat_name}")

        nest_id = int(rows[0]["nest_id"])
        session_row = connection.execute(
            "SELECT id, status FROM flat_scan_sessions WHERE nest_id = ? ORDER BY id DESC LIMIT 1",
            (nest_id,),
        ).fetchone()
        if session_row is None:
            started_at = datetime.now().isoformat(timespec="seconds")
            cursor = connection.execute(
                "INSERT INTO flat_scan_sessions (nest_id, started_at, status) VALUES (?, ?, 'open')",
                (nest_id, started_at),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("Failed to create flat scan session.")
            session_id = int(cursor.lastrowid)
            session_status = "open"
        else:
            session_id = int(session_row["id"])
            session_status = str(session_row["status"] or "open")

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

        return session_id, session_status

    def _load_flat_scan_snapshot(self, dat_name: str) -> dict[str, Any]:
        with get_connection() as connection:
            create_schema(connection)
            rows = self._load_resolved_rows_for_dat(connection, dat_name)
            if not rows:
                raise UiStateError(f"No resolved parts found for {dat_name}")
            self._upsert_monitor_units(connection, rows, dat_name=dat_name)
            session_id, session_status = self._ensure_flat_scan_session(connection, dat_name, rows)
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
                    )
                )
            for sequence in range(scanned_quantity + 1, expected_quantity + 1):
                expected_parts.append(
                    self._build_part_instance(
                        row,
                        sequence,
                        flat_scan_session_id=session_id,
                    )
                )

        return {
            "session_id": session_id,
            "status": session_status,
            "expected_parts": expected_parts,
            "scanned_parts": scanned_parts,
        }

    def _increment_flat_scan_item(self, flat_scan_session_id: int | None, part: dict[str, Any]) -> None:
        if flat_scan_session_id is None:
            return

        with get_connection() as connection:
            create_schema(connection)
            connection.execute(
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
                (int(flat_scan_session_id), int(part["nest_part_id"])),
            )
            connection.execute(
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
                (
                    "flat_scan",
                    str(part.get("part_number") or ""),
                    str(part.get("part_number") or ""),
                    str(part.get("part_revision") or ""),
                    int(flat_scan_session_id),
                    str(part.get("com_number") or ""),
                ),
            )
            self._touch_monitor_units(connection, [str(part.get("com_number") or "")])
            connection.commit()

    def _ensure_forming_batch(self, connection, dat_name: str, rows: list[Any], *, mark_started: bool) -> tuple[int, str, str, str]:
        if not rows:
            raise UiStateError(f"No forming parts found for {dat_name}")

        batch_row = connection.execute(
            "SELECT id, status, created_at, started_at FROM forming_batches WHERE batch_code = ?",
            (dat_name,),
        ).fetchone()
        if batch_row is None:
            com_numbers = self._extract_com_numbers_from_rows(rows)
            build_date_code = str(rows[0]["build_date_code"] or "") or None
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
                    dat_name,
                    int(rows[0]["nest_id"]),
                    com_numbers[0] if len(com_numbers) == 1 else None,
                    build_date_code,
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

        for row in rows:
            expected_quantity = max(1, int(row["quantity_nested"] or 0))
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
                    int(row["nest_part_id"]),
                    row["matched_part_attribute_id"],
                    row["part_number"],
                    row["part_revision"] or "-",
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

        return batch_id, batch_status, created_at, started_at

    def _load_forming_batch_snapshot(self, dat_name: str, *, mark_started: bool = False) -> dict[str, Any] | None:
        with get_connection() as connection:
            create_schema(connection)
            rows = self._load_resolved_rows_for_dat(connection, dat_name, requires_forming=True)
            if not rows:
                return None
            self._upsert_monitor_units(connection, rows, dat_name=dat_name)
            batch_id, batch_status, created_at, started_at = self._ensure_forming_batch(
                connection,
                dat_name,
                rows,
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
                        forming_batch_id=batch_id,
                    )
                )
            for sequence in range(scanned_quantity + 1, expected_quantity + 1):
                expected_parts.append(
                    self._build_part_instance(
                        row,
                        sequence,
                        forming_batch_id=batch_id,
                    )
                )

        return {
            "batch_id": batch_id,
            "dat_name": dat_name,
            "status": batch_status,
            "queued_at": created_at,
            "loaded_at": started_at,
            "com_numbers": self._extract_com_numbers_from_rows(rows),
            "expected_parts": expected_parts,
            "scanned_parts": scanned_parts,
        }

    def _apply_forming_snapshot_to_state(self, state: dict[str, Any], snapshot: dict[str, Any], *, mark_loaded: bool) -> None:
        dat_name = str(snapshot["dat_name"])
        queue_entry = {
            "dat_name": dat_name,
            "queued_at": snapshot.get("queued_at") or datetime.now().isoformat(timespec="seconds"),
            "parts": list(snapshot["expected_parts"]),
            "status": "loaded" if mark_loaded else "queued",
        }
        queue_index = next(
            (index for index, item in enumerate(state.get("formed_queue", [])) if str(item.get("dat_name") or "") == dat_name),
            None,
        )
        if queue_index is None:
            if snapshot["expected_parts"]:
                state.setdefault("formed_queue", []).append(queue_entry)
        else:
            if snapshot["expected_parts"]:
                state["formed_queue"][queue_index] = queue_entry
            else:
                state["formed_queue"].pop(queue_index)

        active_entry = {
            "dat_name": dat_name,
            "loaded_at": snapshot.get("loaded_at") or datetime.now().isoformat(timespec="seconds"),
            "forming_batch_id": snapshot["batch_id"],
            "com_numbers": list(snapshot["com_numbers"]),
            "expected_parts": list(snapshot["expected_parts"]),
            "scanned_parts": list(snapshot["scanned_parts"]),
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
            state["nest_data"] = cleaned
            state["update_target"] = ""
            snapshot = self._load_flat_scan_snapshot(cleaned)
            state["flat_scan_session_id"] = snapshot["session_id"]
            state["flat_scan_status"] = snapshot["status"]
            state["expected_parts"] = list(snapshot["expected_parts"])
            state["scanned_parts"] = list(snapshot["scanned_parts"])
            self._queue_formed_from_nest(state, cleaned)
            if state["expected_parts"]:
                state["active_field"] = "part_scan"
                state["message"] = "Continue scanning parts" if state["scanned_parts"] else "Start scanning parts"
                state["message_level"] = "info"
            else:
                state["active_field"] = "nest_data"
                state["message"] = f"All parts already scanned for {cleaned}. Scan the next DAT."
                state["message_level"] = "success"
        elif field_name == "part_scan":
            if not state.get("nest_data"):
                raise UiStateError("Scan nest data before scanning parts.")
            self._scan_part_into_state(state, cleaned)
        else:
            raise UiStateError(f"Unsupported field {field_name}")

        self.write(state)
        return state

    def _queue_formed_from_nest(self, state: dict[str, Any], dat_name: str) -> None:
        snapshot = self._load_forming_batch_snapshot(dat_name, mark_started=False)
        if snapshot is None:
            return
        self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=False)

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

    def _scan_part_into_state(self, state: dict[str, Any], part_number: str) -> None:
        target_index = next(
            (index for index, part in enumerate(state["expected_parts"]) if part["part_number"] == part_number),
            None,
        )
        target = state["expected_parts"][target_index] if target_index is not None else None
        if target is None:
            raise UiStateError(f"Part {part_number} is not expected or is already complete.")

        state["expected_parts"].pop(target_index)
        state["scanned_parts"].append(target)
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
            snapshot = self._load_forming_batch_snapshot(dat_name, mark_started=False)
            if snapshot is None:
                continue
            is_loaded = any(
                str(item.get("dat_name") or "").strip() == dat_name
                for item in state.get("formed_active_lists", [])
            )
            self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=is_loaded)
            changed = True
        if changed:
            self.write(state)
        return {
            "queue": [item for item in state["formed_queue"] if item.get("status") == "queued"],
            "lists": state["formed_active_lists"],
            "active_field": state["formed_active_field"],
            "message": state["formed_message"],
            "message_level": state["formed_message_level"],
        }

    def formed_scan_dat(self, dat_token: str) -> dict[str, Any]:
        state = self.read()
        token = self._normalize_dat_token(dat_token)
        if not token:
            raise UiStateError("DAT token was blank.")

        snapshot = self._load_forming_batch_snapshot(token, mark_started=True)
        if snapshot is None:
            raise UiStateError(f"No forming parts found for {token}")

        self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=True)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed list for {snapshot['dat_name']}. Scan formed parts."
        state["formed_message_level"] = "info"
        self.write(state)
        return state

    def _try_load_queued_dat_into_formed(self, state: dict[str, Any], dat_token: str) -> tuple[bool, str | None]:
        token = self._normalize_dat_token(dat_token)
        if not token:
            return False, None

        snapshot = self._load_forming_batch_snapshot(token, mark_started=True)
        if snapshot is None:
            return False, token

        self._apply_forming_snapshot_to_state(state, snapshot, mark_loaded=True)
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed list for {snapshot['dat_name']}. Scan formed parts."
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

        return {
            "total_parts": int(row["total_parts"] or 0) if row is not None else 0,
            "total_forming_parts": int(row["total_forming_parts"] or 0) if row is not None else 0,
            "dat_count": int(row["dat_count"] or 0) if row is not None else 0,
            "distinct_parts": int(row["distinct_parts"] or 0) if row is not None else 0,
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
                self.write(state)
                return state
            raise UiStateError(f"Part {cleaned} is not expected in any active formed list.")

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

    def complete_current_batch(self) -> int:
        state = self.read()
        if state["expected_parts"]:
            raise UiStateError("Cannot complete yet. There are still expected parts remaining.")
        if not state["scanned_parts"]:
            raise UiStateError("No scanned parts to complete.")

        now = datetime.now().isoformat(timespec="seconds")
        archive_rows = self._read_completed()
        for part in state["scanned_parts"]:
            archive_rows.append(
                {
                    "completed_at": now,
                    "machine": state.get("machine_code") or "",
                    "user": state.get("user_code") or "",
                    "location": state.get("location_code") or "",
                    "nest_data": state.get("nest_data") or "",
                    "part_number": part.get("part_number"),
                    "part_revision": part.get("part_revision"),
                    "com_number": part.get("com_number"),
                    "f_flag": bool(part.get("requires_forming")),
                }
            )

        count = len(state["scanned_parts"])
        self._write_completed(archive_rows)

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

        reset_state = self._default_state()
        reset_state["machine_code"] = state.get("machine_code", "")
        reset_state["user_code"] = state.get("user_code", "")
        reset_state["location_code"] = state.get("location_code", "")
        reset_state["formed_queue"] = state.get("formed_queue", [])
        reset_state["formed_active_lists"] = state.get("formed_active_lists", [])
        reset_state["formed_active_field"] = state.get("formed_active_field", "dat_token")
        reset_state["formed_message"] = state.get("formed_message", "Scan DAT token to load formed list")
        reset_state["formed_message_level"] = state.get("formed_message_level", "info")
        if reset_state["machine_code"] and reset_state["user_code"] and reset_state["location_code"]:
            reset_state["active_field"] = "nest_data"
            reset_state["message"] = f"Batch completed. Archived {count} parts. Scan NEST DATA."
        else:
            reset_state["active_field"] = "machine_code"
            reset_state["message"] = f"Batch completed. Archived {count} parts. Scan MACHINE."
        reset_state["message_level"] = "success"
        self.write(reset_state)
        return count

    def clear_session_data(self) -> dict[str, Any]:
        state = self._default_state()
        self.write(state)
        return state

    def clear_development_progress(self) -> None:
        with get_connection() as connection:
            create_schema(connection)
            connection.executescript(
                """
                DELETE FROM scan_events;
                DELETE FROM flat_scan_items;
                DELETE FROM flat_scan_sessions;
                DELETE FROM forming_batch_items;
                DELETE FROM forming_batches;
                DELETE FROM monitor_unit_sources;
                DELETE FROM monitor_units;
                DELETE FROM sqlite_sequence WHERE name IN (
                    'scan_events',
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
        self._write_completed([])
        self._write_missed([])
        return state

    def get_completed_list(self) -> list[dict[str, Any]]:
        return self._read_completed()

    def get_missed_list(self) -> list[dict[str, Any]]:
        return self._read_missed()

    def clear_completed_list(self) -> int:
        existing = self._read_completed()
        removed = len(existing)
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
