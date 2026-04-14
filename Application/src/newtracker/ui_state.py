from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .db import DATA_DIR, get_connection

UI_STATE_PATH = DATA_DIR / "ui_scan_state.json"
COMPLETED_LIST_PATH = DATA_DIR / "completed_scan_list.json"

class UiStateError(ValueError):
    pass


class UiStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or UI_STATE_PATH
        self.completed_path = COMPLETED_LIST_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.reset()
        if not self.completed_path.exists():
            self._write_completed([])

    def _default_state(self) -> dict[str, Any]:
        return {
            "machine_code": "",
            "user_code": "",
            "location_code": "",
            "update_target": "",
            "nest_data": "",
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
        if not self.path.exists():
            return self._default_state()
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, state: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _read_completed(self) -> list[dict[str, Any]]:
        if not self.completed_path.exists():
            return []
        return json.loads(self.completed_path.read_text(encoding="utf-8"))

    def _write_completed(self, rows: list[dict[str, Any]]) -> None:
        self.completed_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

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

    def load_expected_parts(self, dat_name: str) -> list[dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    part_number,
                    part_revision,
                    quantity_nested,
                    com_number,
                    requires_forming
                FROM resolved_nest_parts
                WHERE barcode_filename = ?
                ORDER BY requires_forming DESC, part_number
                """,
                (dat_name,),
            ).fetchall()

        if not rows:
            raise UiStateError(f"No resolved parts found for {dat_name}")

        expected: list[dict[str, Any]] = []
        for row in rows:
            for sequence in range(max(1, int(row["quantity_nested"] or 0))):
                expected.append(
                    {
                        "part_number": row["part_number"],
                        "part_revision": row["part_revision"] or "-",
                        "com_number": row["com_number"],
                        "requires_forming": bool(row["requires_forming"]),
                        "sequence": sequence + 1,
                    }
                )
        return expected

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
            state["expected_parts"] = self.load_expected_parts(cleaned)
            self._queue_formed_from_nest(state, cleaned)
            state["scanned_parts"] = []
            state["active_field"] = "part_scan"
            state["message"] = "Start scanning parts"
            state["message_level"] = "info"
        elif field_name == "part_scan":
            if not state.get("nest_data"):
                raise UiStateError("Scan nest data before scanning parts.")
            self._scan_part_into_state(state, cleaned)
        else:
            raise UiStateError(f"Unsupported field {field_name}")

        self.write(state)
        return state

    def _queue_formed_from_nest(self, state: dict[str, Any], dat_name: str) -> None:
        formed_parts = [p for p in state["expected_parts"] if bool(p.get("requires_forming"))]
        if not formed_parts:
            return
        state["formed_queue"].append(
            {
                "dat_name": dat_name,
                "queued_at": datetime.now().isoformat(timespec="seconds"),
                "parts": formed_parts,
                "status": "queued",
            }
        )

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

        remaining = len(state["expected_parts"])
        if remaining == 0:
            state["message"] = "All parts scanned. Enter or scan MACHINE to start over or Reset."
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

        queue_index = next(
            (
                index
                for index, item in enumerate(state["formed_queue"])
                if item.get("status") == "queued" and self._normalize_dat_token(str(item.get("dat_name", ""))) == token
            ),
            None,
        )
        if queue_index is None:
            raise UiStateError(f"No queued formed list found for {token}")

        queued = state["formed_queue"][queue_index]
        com_numbers = self._extract_com_numbers(queued["parts"])
        state["formed_queue"][queue_index]["status"] = "loaded"
        state["formed_active_lists"].append(
            {
                "dat_name": queued["dat_name"],
                "loaded_at": datetime.now().isoformat(timespec="seconds"),
                "com_numbers": com_numbers,
                "expected_parts": list(queued["parts"]),
                "scanned_parts": [],
            }
        )
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed list for {queued['dat_name']}. Scan formed parts."
        state["formed_message_level"] = "info"
        self.write(state)
        return state

    def _try_load_queued_dat_into_formed(self, state: dict[str, Any], dat_token: str) -> tuple[bool, str | None]:
        token = self._normalize_dat_token(dat_token)
        if not token:
            return False, None

        queue_index = next(
            (
                index
                for index, item in enumerate(state["formed_queue"])
                if item.get("status") == "queued" and self._normalize_dat_token(str(item.get("dat_name", ""))) == token
            ),
            None,
        )
        if queue_index is None:
            return False, token

        queued = state["formed_queue"][queue_index]
        com_numbers = self._extract_com_numbers(queued["parts"])
        state["formed_queue"][queue_index]["status"] = "loaded"
        state["formed_active_lists"].append(
            {
                "dat_name": queued["dat_name"],
                "loaded_at": datetime.now().isoformat(timespec="seconds"),
                "com_numbers": com_numbers,
                "expected_parts": list(queued["parts"]),
                "scanned_parts": [],
            }
        )
        state["formed_active_field"] = "part_scan"
        state["formed_message"] = f"Loaded formed list for {queued['dat_name']}. Scan formed parts."
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

    def formed_scan_part(self, part_number: str) -> dict[str, Any]:
        state = self.read()
        cleaned = part_number.strip()
        if not cleaned:
            raise UiStateError("Scanned value was blank.")

        loaded_dat, _ = self._try_load_queued_dat_into_formed(state, cleaned)
        if loaded_dat:
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
                matched = True
                matched_dat = dat_list["dat_name"]
                break

        if not matched:
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
        state = self.read()
        state["machine_code"] = ""
        state["user_code"] = ""
        state["location_code"] = ""
        state["update_target"] = ""
        state["active_field"] = "machine_code"
        state["message"] = "Session identity cleared. Scan MACHINE"
        state["message_level"] = "info"
        self.write(state)
        return state

    def clear_runtime_data(self) -> dict[str, Any]:
        state = self._default_state()
        self.write(state)
        self._write_completed([])
        return state

    def get_completed_list(self) -> list[dict[str, Any]]:
        return self._read_completed()

    def clear_completed_list(self) -> int:
        existing = self._read_completed()
        removed = len(existing)
        self._write_completed([])
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
