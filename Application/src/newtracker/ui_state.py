from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .db import DATA_DIR, get_connection
from .schema import create_schema

UI_STATE_PATH = DATA_DIR / "ui_scan_state.json"
COMPLETED_LIST_PATH = DATA_DIR / "completed_scan_list.json"


class UiStateError(ValueError):
    pass


class UiStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or UI_STATE_PATH
        self.completed_path = COMPLETED_LIST_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()
        if not self.path.exists():
            self.reset()
        if not self.completed_path.exists():
            self._write_completed([])

    def _ensure_schema(self) -> None:
        with get_connection() as connection:
            create_schema(connection)
            connection.commit()

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
            "formed_review_mode": False,
            "formed_review_edit_mode": False,
            "formed_review_rows": [],
            "formed_review_missed_rows": [],
            "formed_review_completed_at": "",
            "formed_review_source": "",
            "review_mode": False,
            "review_edit_mode": False,
            "review_rows": [],
            "review_missed_rows": [],
            "review_completed_at": "",
            "review_source": "",
        }

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._default_state()
        state = json.loads(self.path.read_text(encoding="utf-8"))
        merged = self._default_state()
        merged.update(state)
        return merged

    def write(self, state: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _read_completed(self) -> list[dict[str, Any]]:
        if not self.completed_path.exists():
            return []
        return json.loads(self.completed_path.read_text(encoding="utf-8"))

    def _write_completed(self, rows: list[dict[str, Any]]) -> None:
        self.completed_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    @staticmethod
    def _preserve_identity(previous: dict[str, Any], state: dict[str, Any]) -> None:
        state["machine_code"] = previous.get("machine_code", "")
        state["user_code"] = previous.get("user_code", "")
        state["location_code"] = previous.get("location_code", "")

    @staticmethod
    def _preserve_formed(previous: dict[str, Any], state: dict[str, Any]) -> None:
        state["formed_queue"] = previous.get("formed_queue", [])
        state["formed_active_lists"] = previous.get("formed_active_lists", [])
        state["formed_active_field"] = previous.get("formed_active_field", "dat_token")
        state["formed_message"] = previous.get("formed_message", "Scan DAT token to load formed list")
        state["formed_message_level"] = previous.get("formed_message_level", "info")
        state["formed_review_mode"] = previous.get("formed_review_mode", False)
        state["formed_review_edit_mode"] = previous.get("formed_review_edit_mode", False)
        state["formed_review_rows"] = previous.get("formed_review_rows", [])
        state["formed_review_missed_rows"] = previous.get("formed_review_missed_rows", [])
        state["formed_review_completed_at"] = previous.get("formed_review_completed_at", "")
        state["formed_review_source"] = previous.get("formed_review_source", "")

    def reset(self) -> dict[str, Any]:
        previous = self.read() if self.path.exists() else self._default_state()
        state = self._default_state()
        self._preserve_identity(previous, state)
        self._preserve_formed(previous, state)

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
        if state.get("review_mode"):
            raise UiStateError("Save or reset the current review before updating batch identity.")
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
        if state.get("review_mode"):
            raise UiStateError("Save or reset the review before starting another scan batch.")

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
            state["review_mode"] = False
            state["review_edit_mode"] = False
            state["review_rows"] = []
            state["review_missed_rows"] = []
            state["review_completed_at"] = ""
            state["review_source"] = ""
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
            state["message"] = "All parts scanned. Review and send to part tracker."
            state["active_field"] = "review"
            state["message_level"] = "success"
        else:
            state["message"] = f"Accepted {part_number}. {remaining} part scans remaining."
            state["active_field"] = "part_scan"
            state["message_level"] = "success"

    def invalidate_scan(self, message: str) -> dict[str, Any]:
        state = self.read()
        state["message"] = f"INVALID SCAN - {message}"
        state["message_level"] = "error"
        self.write(state)
        return state

    def formed_context(self) -> dict[str, Any]:
        state = self.read()
        total_expected = sum(len(item.get("expected_parts", [])) for item in state["formed_active_lists"])
        total_scanned = sum(len(item.get("scanned_parts", [])) for item in state["formed_active_lists"])
        return {
            "queue": [item for item in state["formed_queue"] if item.get("status") == "queued"],
            "lists": state["formed_active_lists"],
            "active_field": state["formed_active_field"],
            "message": state["formed_message"],
            "message_level": state["formed_message_level"],
            "review_mode": bool(state.get("formed_review_mode")),
            "review_edit_mode": bool(state.get("formed_review_edit_mode")),
            "review_rows": list(state.get("formed_review_rows", [])),
            "review_missed_rows": list(state.get("formed_review_missed_rows", [])),
            "total_expected": total_expected,
            "total_scanned": total_scanned,
            "can_complete": (not state.get("formed_review_mode")) and total_expected == 0 and total_scanned > 0,
            "can_force_complete": (not state.get("formed_review_mode")) and (total_expected > 0 or total_scanned > 0),
        }

    def formed_scan_dat(self, dat_token: str) -> dict[str, Any]:
        state = self.read()
        if state.get("formed_review_mode"):
            raise UiStateError("Save or reset the formed review before loading another DAT token.")
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
        if state.get("formed_review_mode"):
            raise UiStateError("Save or reset the formed review before scanning more formed parts.")
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
        state["formed_message"] = f"INVALID SCAN - {message}"
        state["formed_message_level"] = "error"
        self.write(state)
        return state

    def _build_formed_review_row(self, dat_name: str, part: dict[str, Any], status: str) -> dict[str, Any]:
        state = self.read()
        return {
            "machine": state.get("machine_code") or "",
            "user": state.get("user_code") or "",
            "location": state.get("location_code") or "",
            "nest_data": dat_name,
            "part_number": str(part.get("part_number") or "").strip(),
            "part_revision": str(part.get("part_revision") or "").strip(),
            "com_number": str(part.get("com_number") or "").strip(),
            "f_flag": self._coerce_flag(part.get("requires_forming")),
            "status": status,
        }

    def _build_blank_formed_review_row(self, state: dict[str, Any]) -> dict[str, Any]:
        default_nest = ""
        if state.get("formed_review_rows"):
            default_nest = str(state["formed_review_rows"][0].get("nest_data") or "")
        elif state.get("formed_active_lists"):
            default_nest = str(state["formed_active_lists"][0].get("dat_name") or "")
        return {
            "machine": state.get("machine_code") or "",
            "user": state.get("user_code") or "",
            "location": state.get("location_code") or "",
            "nest_data": default_nest,
            "part_number": "",
            "part_revision": "",
            "com_number": "",
            "f_flag": False,
            "status": "scanned",
        }

    def _prepare_formed_review(self, force_complete: bool) -> dict[str, Any]:
        state = self.read()
        active_lists = list(state.get("formed_active_lists", []))
        if not active_lists:
            raise UiStateError("No active formed lists are loaded to review.")

        total_expected = sum(len(item.get("expected_parts", [])) for item in active_lists)
        total_scanned = sum(len(item.get("scanned_parts", [])) for item in active_lists)
        if not force_complete and total_expected > 0:
            raise UiStateError("Cannot complete yet. There are still expected formed parts remaining.")
        if total_expected == 0 and total_scanned == 0:
            raise UiStateError("No formed rows are loaded to review.")

        state["formed_review_mode"] = True
        state["formed_review_edit_mode"] = False
        state["formed_review_completed_at"] = datetime.now().isoformat(timespec="seconds")
        state["formed_review_source"] = "force_complete" if force_complete else "complete"
        state["formed_review_rows"] = []
        state["formed_review_missed_rows"] = []

        for dat_list in active_lists:
            dat_name = str(dat_list.get("dat_name") or "")
            state["formed_review_rows"].extend(
                self._build_formed_review_row(dat_name, part, "scanned")
                for part in dat_list.get("scanned_parts", [])
            )
            if force_complete:
                state["formed_review_missed_rows"].extend(
                    self._build_formed_review_row(dat_name, part, "missed")
                    for part in dat_list.get("expected_parts", [])
                )

        state["formed_active_field"] = "review"
        if force_complete:
            state["formed_message"] = (
                f"Review {len(state['formed_review_rows'])} scanned rows and {len(state['formed_review_missed_rows'])} missed formed rows before sending."
            )
            state["formed_message_level"] = "info"
        else:
            state["formed_message"] = (
                f"Review {len(state['formed_review_rows'])} formed rows before sending to part tracker."
            )
            state["formed_message_level"] = "success"

        self.write(state)
        return state

    def formed_complete_current_batch(self) -> dict[str, Any]:
        return self._prepare_formed_review(force_complete=False)

    def formed_force_complete_current_batch(self) -> dict[str, Any]:
        return self._prepare_formed_review(force_complete=True)

    def update_formed_review_from_form(self, form: Mapping[str, Any]) -> dict[str, Any]:
        state = self.read()
        if not state.get("formed_review_mode"):
            raise UiStateError("No formed review session is active.")

        state["formed_review_rows"] = self._parse_review_rows_from_form(
            form,
            "formed_scanned",
            state.get("formed_review_rows", []),
        )
        state["formed_review_missed_rows"] = self._parse_review_rows_from_form(
            form,
            "formed_missed",
            state.get("formed_review_missed_rows", []),
        )
        self.write(state)
        return state

    def enable_formed_review_edit(self, form: Mapping[str, Any] | None = None) -> dict[str, Any]:
        state = self.read()
        if not state.get("formed_review_mode"):
            raise UiStateError("No formed review session is active.")
        if form is not None:
            state = self.update_formed_review_from_form(form)
        state["formed_review_edit_mode"] = True
        state["formed_message"] = "Edit formed review rows as needed, then save and send to part tracker."
        state["formed_message_level"] = "info"
        self.write(state)
        return state

    def add_manual_formed_review_row(self, form: Mapping[str, Any] | None = None) -> dict[str, Any]:
        state = self.read()
        if not state.get("formed_review_mode"):
            raise UiStateError("No formed review session is active.")
        if form is not None:
            state = self.update_formed_review_from_form(form)
        state["formed_review_edit_mode"] = True
        state["formed_review_rows"].append(self._build_blank_formed_review_row(state))
        state["formed_message"] = "Manual formed row added. Fill in the fields, then save."
        state["formed_message_level"] = "info"
        self.write(state)
        return state

    def save_formed_review(self, form: Mapping[str, Any]) -> tuple[int, int]:
        state = self.update_formed_review_from_form(form)
        completed_rows = list(state.get("formed_review_rows", []))
        missed_rows = list(state.get("formed_review_missed_rows", []))

        self._validate_review_rows(completed_rows, "Formed completed")
        self._validate_review_rows(missed_rows, "Formed missed")

        completed_at = state.get("formed_review_completed_at") or datetime.now().isoformat(timespec="seconds")
        completed_count = self._append_completed_rows(completed_rows, completed_at)
        missed_count = self._insert_missed_rows(missed_rows, completed_at)

        state["formed_active_lists"] = []
        state["formed_queue"] = [item for item in state.get("formed_queue", []) if item.get("status") == "queued"]
        state["formed_active_field"] = "dat_token"
        state["formed_review_mode"] = False
        state["formed_review_edit_mode"] = False
        state["formed_review_rows"] = []
        state["formed_review_missed_rows"] = []
        state["formed_review_completed_at"] = ""
        state["formed_review_source"] = ""
        state["formed_message"] = (
            f"Sent {completed_count} formed rows to part tracker and {missed_count} to missed scans."
        )
        state["formed_message_level"] = "success"
        self.write(state)
        return completed_count, missed_count

    @staticmethod
    def _coerce_flag(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().upper()
        return text in {"1", "Y", "YES", "TRUE", "T", "FORMED", "REQUIRES FORMING"}

    def _build_review_row(self, state: dict[str, Any], part: dict[str, Any], status: str) -> dict[str, Any]:
        return {
            "machine": state.get("machine_code") or "",
            "user": state.get("user_code") or "",
            "location": state.get("location_code") or "",
            "nest_data": state.get("nest_data") or "",
            "part_number": str(part.get("part_number") or "").strip(),
            "part_revision": str(part.get("part_revision") or "").strip(),
            "com_number": str(part.get("com_number") or "").strip(),
            "f_flag": self._coerce_flag(part.get("requires_forming")),
            "status": status,
        }

    def _blank_review_row(self, state: dict[str, Any], status: str = "scanned") -> dict[str, Any]:
        return {
            "machine": state.get("machine_code") or "",
            "user": state.get("user_code") or "",
            "location": state.get("location_code") or "",
            "nest_data": state.get("nest_data") or "",
            "part_number": "",
            "part_revision": "",
            "com_number": "",
            "f_flag": False,
            "status": status,
        }

    def _prepare_review(self, force_complete: bool) -> dict[str, Any]:
        state = self.read()
        if not force_complete and state["expected_parts"]:
            raise UiStateError("Cannot complete yet. There are still expected parts remaining.")
        if not state["scanned_parts"] and not state["expected_parts"]:
            raise UiStateError("No batch rows are loaded to review.")

        now = datetime.now().isoformat(timespec="seconds")
        state["review_mode"] = True
        state["review_edit_mode"] = False
        state["review_completed_at"] = now
        state["review_source"] = "force_complete" if force_complete else "complete"
        state["review_rows"] = [self._build_review_row(state, part, "scanned") for part in state["scanned_parts"]]
        state["review_missed_rows"] = [
            self._build_review_row(state, part, "missed") for part in state["expected_parts"]
        ] if force_complete else []
        state["active_field"] = "review"

        if force_complete:
            state["message"] = (
                f"Review {len(state['review_rows'])} scanned rows and {len(state['review_missed_rows'])} missed rows before sending."
            )
            state["message_level"] = "info"
        else:
            state["message"] = f"Review {len(state['review_rows'])} scanned rows before sending to part tracker."
            state["message_level"] = "success"

        self.write(state)
        return state

    def complete_current_batch(self) -> dict[str, Any]:
        return self._prepare_review(force_complete=False)

    def force_complete_current_batch(self) -> dict[str, Any]:
        return self._prepare_review(force_complete=True)

    @staticmethod
    def _row_field(prefix: str, index: int, field_name: str) -> str:
        return f"{prefix}__{index}__{field_name}"

    def _parse_review_rows_from_form(
        self,
        form: Mapping[str, Any],
        prefix: str,
        fallback_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        try:
            count = int(str(form.get(f"{prefix}_count", len(fallback_rows))) or len(fallback_rows))
        except ValueError:
            count = len(fallback_rows)

        rows: list[dict[str, Any]] = []
        for index in range(count):
            fallback = fallback_rows[index] if index < len(fallback_rows) else {
                "machine": "",
                "user": "",
                "location": "",
                "nest_data": "",
                "part_number": "",
                "part_revision": "",
                "com_number": "",
                "f_flag": False,
            }
            rows.append(
                {
                    "machine": str(form.get(self._row_field(prefix, index, "machine"), fallback.get("machine", ""))).strip(),
                    "user": str(form.get(self._row_field(prefix, index, "user"), fallback.get("user", ""))).strip(),
                    "location": str(form.get(self._row_field(prefix, index, "location"), fallback.get("location", ""))).strip(),
                    "nest_data": str(form.get(self._row_field(prefix, index, "nest_data"), fallback.get("nest_data", ""))).strip(),
                    "part_number": str(form.get(self._row_field(prefix, index, "part_number"), fallback.get("part_number", ""))).strip(),
                    "part_revision": str(form.get(self._row_field(prefix, index, "part_revision"), fallback.get("part_revision", ""))).strip(),
                    "com_number": str(form.get(self._row_field(prefix, index, "com_number"), fallback.get("com_number", ""))).strip(),
                    "f_flag": self._coerce_flag(form.get(self._row_field(prefix, index, "f_flag"), fallback.get("f_flag", False))),
                    "status": "missed" if prefix == "missed" else "scanned",
                }
            )
        return rows

    def update_review_from_form(self, form: Mapping[str, Any]) -> dict[str, Any]:
        state = self.read()
        if not state.get("review_mode"):
            raise UiStateError("No review session is active.")

        state["review_rows"] = self._parse_review_rows_from_form(form, "scanned", state.get("review_rows", []))
        state["review_missed_rows"] = self._parse_review_rows_from_form(form, "missed", state.get("review_missed_rows", []))
        self.write(state)
        return state

    def enable_review_edit(self, form: Mapping[str, Any] | None = None) -> dict[str, Any]:
        state = self.read()
        if not state.get("review_mode"):
            raise UiStateError("No review session is active.")
        if form is not None:
            state = self.update_review_from_form(form)
        state["review_edit_mode"] = True
        state["message"] = "Edit review rows as needed, then save and send to part tracker."
        state["message_level"] = "info"
        self.write(state)
        return state

    def add_manual_review_row(self, form: Mapping[str, Any] | None = None) -> dict[str, Any]:
        state = self.read()
        if not state.get("review_mode"):
            raise UiStateError("No review session is active.")
        if form is not None:
            state = self.update_review_from_form(form)
        state["review_edit_mode"] = True
        state["review_rows"].append(self._blank_review_row(state, "scanned"))
        state["message"] = "Manual row added. Fill in the fields, then save."
        state["message_level"] = "info"
        self.write(state)
        return state

    @staticmethod
    def _validate_review_rows(rows: list[dict[str, Any]], label: str) -> None:
        for index, row in enumerate(rows, start=1):
            if not str(row.get("part_number") or "").strip():
                raise UiStateError(f"{label} row {index} needs a part number before saving.")

    def _append_completed_rows(self, rows: list[dict[str, Any]], completed_at: str) -> int:
        archive_rows = self._read_completed()
        for row in rows:
            archive_rows.append(
                {
                    "completed_at": completed_at,
                    "machine": row.get("machine", ""),
                    "user": row.get("user", ""),
                    "location": row.get("location", ""),
                    "nest_data": row.get("nest_data", ""),
                    "part_number": row.get("part_number", ""),
                    "part_revision": row.get("part_revision", ""),
                    "com_number": row.get("com_number", ""),
                    "f_flag": bool(row.get("f_flag")),
                }
            )
        self._write_completed(archive_rows)
        return len(rows)

    def _insert_missed_rows(self, rows: list[dict[str, Any]], completed_at: str) -> int:
        if not rows:
            return 0
        with get_connection() as connection:
            connection.executemany(
                """
                INSERT INTO missed_scans (
                    review_completed_at,
                    machine_code,
                    user_code,
                    location_code,
                    nest_data,
                    part_number,
                    part_revision,
                    com_number,
                    requires_forming,
                    reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        completed_at,
                        row.get("machine", ""),
                        row.get("user", ""),
                        row.get("location", ""),
                        row.get("nest_data", ""),
                        row.get("part_number", ""),
                        row.get("part_revision", ""),
                        row.get("com_number", ""),
                        1 if row.get("f_flag") else 0,
                        "force_complete",
                    )
                    for row in rows
                ],
            )
            connection.commit()
        return len(rows)

    def save_review(self, form: Mapping[str, Any]) -> tuple[int, int]:
        state = self.update_review_from_form(form)
        completed_rows = list(state.get("review_rows", []))
        missed_rows = list(state.get("review_missed_rows", []))

        self._validate_review_rows(completed_rows, "Completed")
        self._validate_review_rows(missed_rows, "Missed")

        completed_at = state.get("review_completed_at") or datetime.now().isoformat(timespec="seconds")
        completed_count = self._append_completed_rows(completed_rows, completed_at)
        missed_count = self._insert_missed_rows(missed_rows, completed_at)

        reset_state = self._default_state()
        self._preserve_identity(state, reset_state)
        self._preserve_formed(state, reset_state)

        if reset_state["machine_code"] and reset_state["user_code"] and reset_state["location_code"]:
            reset_state["active_field"] = "nest_data"
            reset_state["message"] = (
                f"Sent {completed_count} rows to part tracker and {missed_count} to missed scans. Scan NEST DATA."
            )
        else:
            reset_state["active_field"] = "machine_code"
            reset_state["message"] = (
                f"Sent {completed_count} rows to part tracker and {missed_count} to missed scans. Scan MACHINE."
            )
        reset_state["message_level"] = "success"
        self.write(reset_state)
        return completed_count, missed_count

    def clear_session_data(self) -> dict[str, Any]:
        state = self.read()
        state["machine_code"] = ""
        state["user_code"] = ""
        state["location_code"] = ""
        state["update_target"] = ""
        state["nest_data"] = ""
        state["expected_parts"] = []
        state["scanned_parts"] = []
        state["review_mode"] = False
        state["review_edit_mode"] = False
        state["review_rows"] = []
        state["review_missed_rows"] = []
        state["review_completed_at"] = ""
        state["review_source"] = ""
        state["formed_review_mode"] = False
        state["formed_review_edit_mode"] = False
        state["formed_review_rows"] = []
        state["formed_review_missed_rows"] = []
        state["formed_review_completed_at"] = ""
        state["formed_review_source"] = ""
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

    def get_missed_list(self) -> list[dict[str, Any]]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    review_completed_at,
                    machine_code,
                    user_code,
                    location_code,
                    nest_data,
                    part_number,
                    part_revision,
                    com_number,
                    requires_forming,
                    reason,
                    created_at
                FROM missed_scans
                ORDER BY id DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def clear_completed_list(self) -> int:
        existing = self._read_completed()
        removed = len(existing)
        self._write_completed([])
        return removed

    def clear_missed_list(self) -> int:
        with get_connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM missed_scans").fetchone()[0]
            connection.execute("DELETE FROM missed_scans")
            connection.commit()
        return int(count)

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
