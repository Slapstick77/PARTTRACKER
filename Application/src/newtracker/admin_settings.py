from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, cast
from werkzeug.security import check_password_hash, generate_password_hash

from .db import APP_ROOT, get_connection
from .error_reports import default_error_report_directory, resolve_error_report_directory, write_error_report
from .importer import clear_scan_cache, import_paths, is_immutable_source_path
from .persistence import atomic_write_json, atomic_write_text, read_json_file
from .schema import create_schema
from .ui_state import UiStateStore

ADMIN_SETTINGS_PATH = APP_ROOT / "data" / "admin_settings.json"
IMPORT_ERROR_LOG_PATH = APP_ROOT / "data" / "import_error.log"
LEGACY_ADMIN_USERNAME = "admin"
LEGACY_ADMIN_PASSWORD = "password"

DEFAULT_SOURCE_FOLDERS = {
    "amada": {
        "label": "Amada",
        "test_path": "",
        "production_path": "",
        "use_production": True,
    },
    "emk1": {
        "label": "EMK1",
        "test_path": "",
        "production_path": "",
        "use_production": True,
    },
    "laser": {
        "label": "Laser",
        "test_path": "",
        "production_path": "",
        "use_production": True,
    },
    "programming": {
        "label": "Programming Folders",
        "test_path": "",
        "production_path": "",
        "use_production": True,
    },
}

MAIN_SCANNER_AUTO_MODE_OFF = "off"
MAIN_SCANNER_AUTO_MODE_AUTO_COMPLETE = "auto_complete"
MAIN_SCANNER_AUTO_MODE_FULL_AUTO = "full_auto"
VALID_MAIN_SCANNER_AUTO_MODES = {
    MAIN_SCANNER_AUTO_MODE_OFF,
    MAIN_SCANNER_AUTO_MODE_AUTO_COMPLETE,
    MAIN_SCANNER_AUTO_MODE_FULL_AUTO,
}

_SETTINGS_LOCK = threading.RLock()
_IMPORT_LOCK = threading.Lock()
_MONITOR_LOCK = threading.Lock()
_MONITOR_STARTED = False
_JOB_THREAD: threading.Thread | None = None
_IMPORT_MONITOR_STATE: dict[str, Any] | None = None


class AdminSettingsError(ValueError):
    pass


class AdminSettingsStore:
    def __init__(self, path: Path | None = None) -> None:
        global _IMPORT_MONITOR_STATE
        self.path = path or ADMIN_SETTINGS_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.write(self._default_state())
        else:
            self.write(self.read())
        if _IMPORT_MONITOR_STATE is None:
            _IMPORT_MONITOR_STATE = self._default_import_monitor()

    @staticmethod
    def _default_import_monitor() -> dict[str, Any]:
        return {
            "status": "idle",
            "trigger": "manual",
            "phase": "Idle",
            "message": "No import running.",
            "started_at": "",
            "completed_at": "",
            "current_file": "",
            "current_step": 0,
            "total_steps": 0,
            "progress_percent": 0,
            "processed": 0,
            "skipped": 0,
            "errors": 0,
            "missing_files": 0,
            "total_supported_files": 0,
            "nest_files": 0,
            "dat_files": 0,
            "dat_groups": 0,
            "duplicate_dat_files": 0,
            "filtered_old_files": 0,
            "unstable_recent_files": 0,
            "scanned_roots": 0,
            "total_roots": 0,
            "discovered_supported_files": 0,
            "active_paths": [],
            "missing_paths": [],
            "last_error": "",
        }

    @staticmethod
    def _compute_progress_percent(monitor: dict[str, Any]) -> int:
        total_steps = int(monitor.get("total_steps") or 0)
        current_step = int(monitor.get("current_step") or 0)
        total_roots = int(monitor.get("total_roots") or 0)
        scanned_roots = int(monitor.get("scanned_roots") or 0)
        phase = str(monitor.get("phase") or "")
        if total_steps > 0:
            return min(100, max(15, int(15 + ((current_step / total_steps) * 85))))
        if phase == "Scanning folders" and total_roots > 0:
            return min(90, max(5, int((scanned_roots / total_roots) * 100)))
        return 0 if monitor.get("status") == "idle" else int(monitor.get("progress_percent") or 0)

    @staticmethod
    def _default_run_result(message: str) -> dict[str, Any]:
        return {
            "status": "idle",
            "trigger": "manual",
            "message": message,
            "processed": 0,
            "skipped": 0,
            "started_at": "",
            "completed_at": "",
            "active_paths": [],
            "missing_paths": [],
        }

    @staticmethod
    def _default_security_state() -> dict[str, Any]:
        configured_username = os.getenv("NEWTRACKER_ADMIN_USERNAME", "").strip() or LEGACY_ADMIN_USERNAME
        configured_password = os.getenv("NEWTRACKER_ADMIN_PASSWORD", "").strip() or LEGACY_ADMIN_PASSWORD
        return {
            "admin_username": configured_username,
            "admin_password_hash": generate_password_hash(configured_password),
            "secret_key": secrets.token_hex(32),
            "password_is_temporary": configured_password == LEGACY_ADMIN_PASSWORD,
        }

    def _default_state(self) -> dict[str, Any]:
        return {
            "poll_interval_minutes": 0,
            "scanner_auto_mode": MAIN_SCANNER_AUTO_MODE_OFF,
            "debug_enabled": False,
            "error_report_folder": str(default_error_report_directory()),
            "folders": {key: dict(value) for key, value in DEFAULT_SOURCE_FOLDERS.items()},
            "last_import": self._default_run_result("No import has been run yet."),
            "security": self._default_security_state(),
        }

    def read(self) -> dict[str, Any]:
        with _SETTINGS_LOCK:
            state = self._default_state()
            if self.path.exists():
                saved = read_json_file(self.path, self._default_state, quarantine_corrupt=True)
                if not isinstance(saved, dict):
                    return state
                state.update({key: value for key, value in saved.items() if key not in {"folders", "security"}})
                for folder_key, folder_value in saved.get("folders", {}).items():
                    if folder_key in state["folders"]:
                        state["folders"][folder_key].update(folder_value)
                security = saved.get("security", {})
                if isinstance(security, Mapping):
                    state["security"].update(dict(security))
            return state

    def write(self, state: dict[str, Any]) -> None:
        with _SETTINGS_LOCK:
            persisted = {
                "poll_interval_minutes": state.get("poll_interval_minutes", 0),
                "scanner_auto_mode": self.scanner_auto_mode(state),
                "debug_enabled": self.debug_enabled(state),
                "error_report_folder": self.error_report_folder(state),
                "folders": state.get("folders", {}),
                "last_import": state.get("last_import", self._default_state()["last_import"]),
                "security": state.get("security", self._default_state()["security"]),
            }
            atomic_write_json(self.path, persisted)

    def scanner_auto_mode(self, state: dict[str, Any] | None = None) -> str:
        current = state or self.read()
        raw = str(current.get("scanner_auto_mode") or MAIN_SCANNER_AUTO_MODE_OFF).strip().lower()
        if raw not in VALID_MAIN_SCANNER_AUTO_MODES:
            return MAIN_SCANNER_AUTO_MODE_OFF
        return raw

    def debug_enabled(self, state: dict[str, Any] | None = None) -> bool:
        current = state or self.read()
        return bool(current.get("debug_enabled", False))

    def error_report_folder(self, state: dict[str, Any] | None = None) -> str:
        current = state or self.read()
        return str(current.get("error_report_folder") or default_error_report_directory())

    def error_report_directory(self, state: dict[str, Any] | None = None) -> Path:
        current = state or self.read()
        return resolve_error_report_directory(self.error_report_folder(current))

    def save_error_report(
        self,
        *,
        category: str,
        summary: str,
        traceback_text: str = "",
        request_info: Mapping[str, Any] | None = None,
        extra: Mapping[str, Any] | None = None,
        force: bool = False,
        raise_on_error: bool = False,
        state: dict[str, Any] | None = None,
    ) -> Path | None:
        current = state or self.read()
        if not force and not self.debug_enabled(current):
            return None
        try:
            return write_error_report(
                directory=self.error_report_directory(current),
                category=category,
                summary=summary,
                traceback_text=traceback_text,
                request_info=request_info,
                extra=extra,
            )
        except OSError:
            if raise_on_error:
                raise
            return None

    def admin_username(self, state: dict[str, Any] | None = None) -> str:
        current = state or self.read()
        return str(current.get("security", {}).get("admin_username") or LEGACY_ADMIN_USERNAME)

    def secret_key(self, state: dict[str, Any] | None = None) -> str:
        current = state or self.read()
        return str(current.get("security", {}).get("secret_key") or self._default_security_state()["secret_key"])

    def password_is_temporary(self, state: dict[str, Any] | None = None) -> bool:
        current = state or self.read()
        return bool(current.get("security", {}).get("password_is_temporary", False))

    def authenticate_admin(self, username: str, password: str, state: dict[str, Any] | None = None) -> bool:
        current = state or self.read()
        stored_username = self.admin_username(current)
        stored_hash = str(current.get("security", {}).get("admin_password_hash") or "")
        if username != stored_username or not stored_hash:
            return False
        return check_password_hash(stored_hash, password)

    def update_security_from_form(self, form: Mapping[str, Any]) -> dict[str, Any]:
        state = self.read()
        current_password = str(form.get("current_password", ""))
        if not self.authenticate_admin(self.admin_username(state), current_password, state):
            raise AdminSettingsError("Current admin password is incorrect.")

        new_username = str(form.get("admin_username", "")).strip() or self.admin_username(state)
        new_password = str(form.get("new_password", ""))
        confirm_password = str(form.get("confirm_password", ""))

        if new_password and new_password != confirm_password:
            raise AdminSettingsError("New admin passwords do not match.")
        if new_password and len(new_password) < 8:
            raise AdminSettingsError("New admin password must be at least 8 characters.")
        if not new_username:
            raise AdminSettingsError("Admin username cannot be blank.")
        if not new_password and new_username == self.admin_username(state):
            raise AdminSettingsError("Enter a new username or a new password.")

        state["security"]["admin_username"] = new_username
        if new_password:
            state["security"]["admin_password_hash"] = generate_password_hash(new_password)
            state["security"]["password_is_temporary"] = False
        self.write(state)
        return state

    def describe_sources(self, state: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        current = state or self.read()
        described: list[dict[str, Any]] = []
        for folder_key, folder in current["folders"].items():
            production_path = (folder.get("production_path") or "").strip()
            use_production = bool(folder.get("use_production"))
            selected_path = production_path if use_production else folder["test_path"]
            selected_path_obj = Path(selected_path) if selected_path else None
            described.append(
                {
                    "key": folder_key,
                    "label": folder["label"],
                    "test_path": folder["test_path"],
                    "production_path": production_path,
                    "use_production": use_production,
                    "selected_mode": "production" if use_production else "test",
                    "selected_path": selected_path,
                    "exists": bool(selected_path_obj and selected_path_obj.exists() and selected_path_obj.is_dir()),
                }
            )
        return described

    def get_active_paths(self, state: dict[str, Any] | None = None) -> tuple[list[Path], list[str]]:
        current = state or self.read()
        active_paths: list[Path] = []
        missing_paths: list[str] = []
        seen: set[str] = set()

        for folder in self.describe_sources(current):
            selected_path = (folder["selected_path"] or "").strip()
            selected_mode = str(folder.get("selected_mode") or "production")
            if not selected_path:
                missing_paths.append(f"{folder['label']}: no {selected_mode} folder set")
                continue

            path = Path(selected_path)
            normalized = str(path).casefold()
            if normalized in seen:
                continue
            seen.add(normalized)

            if path.exists() and path.is_dir():
                active_paths.append(path)
            else:
                missing_paths.append(f"{folder['label']}: {selected_path}")

        return active_paths, missing_paths

    def update_from_form(self, form: Mapping[str, Any]) -> dict[str, Any]:
        state = self.read()

        poll_raw = str(form.get("poll_interval_minutes", "0")).strip()
        try:
            poll_interval = int(poll_raw or "0")
        except ValueError as exc:
            raise AdminSettingsError("Auto-check interval must be a whole number of minutes.") from exc

        if poll_interval < 0:
            raise AdminSettingsError("Auto-check interval cannot be negative.")

        state["poll_interval_minutes"] = poll_interval

        scanner_auto_mode = str(form.get("scanner_auto_mode", MAIN_SCANNER_AUTO_MODE_OFF)).strip().lower()
        if scanner_auto_mode not in VALID_MAIN_SCANNER_AUTO_MODES:
            raise AdminSettingsError("Main scanner auto mode was invalid.")
        state["scanner_auto_mode"] = scanner_auto_mode

        debug_enabled = str(form.get("debug_enabled", "off")).strip().lower() == "on"
        report_directory = resolve_error_report_directory(str(form.get("error_report_folder", "")).strip())
        try:
            report_directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise AdminSettingsError("Error report folder could not be created. Check the path and permissions.") from exc
        if not report_directory.is_dir():
            raise AdminSettingsError("Error report folder must point to a folder.")
        state["debug_enabled"] = debug_enabled
        state["error_report_folder"] = str(report_directory)

        for folder_key in state["folders"]:
            mode = str(form.get(f"source_mode_{folder_key}", "test")).strip().lower()
            production_path = str(form.get(f"production_path_{folder_key}", "")).strip()
            state["folders"][folder_key]["use_production"] = mode == "production"
            state["folders"][folder_key]["production_path"] = production_path

        self.write(state)
        return state

    @staticmethod
    def _encode_path_list(values: list[str]) -> str:
        return json.dumps([str(value) for value in values])

    @staticmethod
    def _decode_path_list(raw_value: Any) -> list[str]:
        if isinstance(raw_value, list):
            return [str(value) for value in raw_value]
        try:
            parsed = json.loads(str(raw_value or "[]"))
        except Exception:
            return []
        if not isinstance(parsed, list):
            return []
        return [str(value) for value in parsed]

    @classmethod
    def _import_run_row_to_result(cls, row: Any) -> dict[str, Any]:
        return {
            "status": str(row["status"] or "idle"),
            "trigger": str(row["trigger"] or "manual"),
            "message": str(row["message"] or "No import has been run yet."),
            "processed": int(row["processed"] or 0),
            "skipped": int(row["skipped"] or 0),
            "errors": int(row["errors"] or 0),
            "missing_files": int(row["missing_files"] or 0),
            "started_at": str(row["started_at"] or ""),
            "completed_at": str(row["completed_at"] or ""),
            "active_paths": cls._decode_path_list(row["active_paths_json"]),
            "missing_paths": cls._decode_path_list(row["missing_paths_json"]),
            "last_error": str(row["last_error"] or ""),
        }

    def begin_import_run(
        self,
        *,
        trigger: str,
        message: str,
        started_at: str,
        active_paths: list[str],
        missing_paths: list[str],
    ) -> int:
        with get_connection() as connection:
            create_schema(connection)
            cursor = connection.execute(
                """
                INSERT INTO import_runs (
                    trigger, status, message, started_at, active_paths_json, missing_paths_json
                ) VALUES (?, 'running', ?, ?, ?, ?)
                """,
                (
                    trigger,
                    message,
                    started_at,
                    self._encode_path_list(active_paths),
                    self._encode_path_list(missing_paths),
                ),
            )
            connection.commit()
        if cursor.lastrowid is None:
            raise RuntimeError("Failed to create import run record.")
        return int(cursor.lastrowid)

    def finish_import_run(
        self,
        run_id: int,
        *,
        status: str,
        message: str,
        started_at: str,
        active_paths: list[str],
        missing_paths: list[str],
        last_error: str = "",
        **stats: Any,
    ) -> None:
        with get_connection() as connection:
            create_schema(connection)
            connection.execute(
                """
                UPDATE import_runs
                SET status = ?,
                    message = ?,
                    started_at = ?,
                    completed_at = ?,
                    active_paths_json = ?,
                    missing_paths_json = ?,
                    processed = ?,
                    skipped = ?,
                    errors = ?,
                    missing_files = ?,
                    total_supported_files = ?,
                    nest_files = ?,
                    dat_files = ?,
                    dat_groups = ?,
                    duplicate_dat_files = ?,
                    filtered_old_files = ?,
                    unstable_recent_files = ?,
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    status,
                    message,
                    started_at,
                    datetime.now().isoformat(timespec="seconds"),
                    self._encode_path_list(active_paths),
                    self._encode_path_list(missing_paths),
                    int(stats.get("processed") or 0),
                    int(stats.get("skipped") or 0),
                    int(stats.get("errors") or 0),
                    int(stats.get("missing_files") or 0),
                    int(stats.get("total_supported_files") or 0),
                    int(stats.get("nest_files") or 0),
                    int(stats.get("dat_files") or 0),
                    int(stats.get("dat_groups") or 0),
                    int(stats.get("duplicate_dat_files") or 0),
                    int(stats.get("filtered_old_files") or 0),
                    int(stats.get("unstable_recent_files") or 0),
                    last_error,
                    run_id,
                ),
            )
            connection.commit()

    def latest_import_result(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        current = state or self.read()
        if self.import_monitor().get("status") == "running":
            return dict(current.get("last_import", self._default_state()["last_import"]))

        try:
            with get_connection() as connection:
                row = connection.execute(
                    "SELECT * FROM import_runs ORDER BY id DESC LIMIT 1"
                ).fetchone()
        except sqlite3.OperationalError:
            return dict(current.get("last_import", self._default_state()["last_import"]))

        if row is not None:
            return self._import_run_row_to_result(row)
        return dict(current.get("last_import", self._default_state()["last_import"]))

    def mark_interrupted_import_runs(self) -> int:
        with get_connection() as connection:
            create_schema(connection)
            running_rows = connection.execute(
                "SELECT id, started_at, active_paths_json, missing_paths_json FROM import_runs WHERE status = 'running'"
            ).fetchall()
            if not running_rows:
                return 0
            interrupted_at = datetime.now().isoformat(timespec="seconds")
            for row in running_rows:
                connection.execute(
                    """
                    UPDATE import_runs
                    SET status = 'interrupted',
                        message = 'Import interrupted before completion.',
                        completed_at = ?,
                        last_error = 'Application stopped before the import completed.',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (interrupted_at, int(row["id"])),
                )
            connection.commit()
        return len(running_rows)

    def update_import_monitor(self, **updates: Any) -> dict[str, Any]:
        global _IMPORT_MONITOR_STATE
        with _SETTINGS_LOCK:
            monitor = dict(_IMPORT_MONITOR_STATE or self._default_import_monitor())
            monitor.update(updates)
            monitor["progress_percent"] = self._compute_progress_percent(monitor)
            _IMPORT_MONITOR_STATE = monitor
            return dict(monitor)

    def start_import_monitor(self, *, trigger: str, active_paths: list[str], missing_paths: list[str], started_at: str) -> dict[str, Any]:
        return self.update_import_monitor(
            status="running",
            trigger=trigger,
            phase="Preparing import",
            message="Preparing folder scan.",
            started_at=started_at,
            completed_at="",
            current_file="",
            current_step=0,
            total_steps=0,
            progress_percent=0,
            processed=0,
            skipped=0,
            errors=0,
            missing_files=0,
            total_supported_files=0,
            nest_files=0,
            dat_files=0,
            dat_groups=0,
            duplicate_dat_files=0,
            filtered_old_files=0,
            unstable_recent_files=0,
            scanned_roots=0,
            total_roots=len(active_paths),
            discovered_supported_files=0,
            active_paths=active_paths,
            missing_paths=missing_paths,
            last_error="",
        )

    def record_import_result(
        self,
        *,
        status: str,
        trigger: str,
        message: str,
        processed: int,
        skipped: int,
        active_paths: list[str],
        missing_paths: list[str],
        started_at: str,
    ) -> dict[str, Any]:
        state = self.read()
        result = {
            "status": status,
            "trigger": trigger,
            "message": message,
            "processed": processed,
            "skipped": skipped,
            "started_at": started_at,
            "completed_at": datetime.now().isoformat(timespec="seconds"),
            "active_paths": active_paths,
            "missing_paths": missing_paths,
        }
        state["last_import"] = result
        self.write(state)
        return state

    def finish_import_monitor(self, *, status: str, message: str, last_error: str = "", **stats: Any) -> dict[str, Any]:
        return self.update_import_monitor(
            status=status,
            message=message,
            completed_at=datetime.now().isoformat(timespec="seconds"),
            last_error=last_error,
            **stats,
        )

    def import_monitor(self) -> dict[str, Any]:
        global _IMPORT_MONITOR_STATE
        with _SETTINGS_LOCK:
            monitor = dict(_IMPORT_MONITOR_STATE or self._default_import_monitor())
            monitor["progress_percent"] = self._compute_progress_percent(monitor)
            _IMPORT_MONITOR_STATE = monitor
            return dict(monitor)


def _progress_updater(store: AdminSettingsStore, trigger: str, active_path_strings: list[str], missing_paths: list[str]):
    def _update(snapshot: dict[str, Any]) -> None:
        current = store.import_monitor()
        store.update_import_monitor(
            status="running",
            trigger=trigger,
            phase=snapshot.get("phase", "Running import"),
            message=snapshot.get("message", "Running import."),
            current_file=snapshot.get("current_file", ""),
            current_step=snapshot.get("current_step", 0),
            total_steps=snapshot.get("total_steps", 0),
            processed=snapshot.get("processed", 0),
            skipped=snapshot.get("skipped", 0),
            errors=snapshot.get("errors", 0),
            missing_files=snapshot.get("missing_files", current.get("missing_files", 0)),
            total_supported_files=snapshot.get("total_supported_files", 0),
            nest_files=snapshot.get("nest_files", 0),
            dat_files=snapshot.get("dat_files", 0),
            dat_groups=snapshot.get("dat_groups", 0),
            duplicate_dat_files=snapshot.get("duplicate_dat_files", 0),
            filtered_old_files=snapshot.get("filtered_old_files", 0),
            unstable_recent_files=snapshot.get("unstable_recent_files", current.get("unstable_recent_files", 0)),
            scanned_roots=snapshot.get("scanned_roots", current.get("scanned_roots", 0)),
            total_roots=snapshot.get("total_roots", current.get("total_roots", len(active_path_strings))),
            discovered_supported_files=snapshot.get("discovered_supported_files", current.get("discovered_supported_files", 0)),
            active_paths=active_path_strings,
            missing_paths=missing_paths,
        )

    return _update


def _warning_reporter(
    store: AdminSettingsStore,
    *,
    trigger: str,
    started_at: str,
    active_paths: list[str],
    missing_paths: list[str],
):
    def _report(payload: Mapping[str, Any]) -> None:
        summary = str(payload.get("summary") or "Import warning")
        store.save_error_report(
            category=str(payload.get("category") or "import-warning"),
            summary=summary,
            traceback_text=str(payload.get("traceback_text") or ""),
            extra={
                "trigger": trigger,
                "started_at": started_at,
                "active_paths": active_paths,
                "missing_paths": missing_paths,
                **dict(payload.get("extra") or {}),
            },
            force=True,
            state=store.read(),
        )

    return _report


def _normal_import_changed_since() -> datetime | None:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT MAX(processed_at) AS latest FROM processed_files WHERE status = 'processed'"
        ).fetchone()

    latest = str(row["latest"] or "").strip() if row else ""
    if not latest:
        return None

    try:
        latest_dt = datetime.fromisoformat(latest.replace(" ", "T"))
    except ValueError:
        return None

    return latest_dt - timedelta(minutes=5)


def run_import_cycle(store: AdminSettingsStore, *, trigger: str) -> dict[str, Any]:
    started_at = datetime.now().isoformat(timespec="seconds")
    with _IMPORT_LOCK:
        state = store.read()
        critical_missing_paths = [
            str(folder["selected_path"])
            for folder in store.describe_sources(state)
            if folder.get("selected_path")
            and not folder.get("exists")
            and is_immutable_source_path(Path(str(folder["selected_path"])))
        ]
        active_paths, missing_paths = store.get_active_paths(state)
        active_path_strings = [str(path) for path in active_paths]
        run_id = store.begin_import_run(
            trigger=trigger,
            message="Preparing folder scan.",
            started_at=started_at,
            active_paths=active_path_strings,
            missing_paths=missing_paths,
        )
        store.start_import_monitor(
            trigger=trigger,
            active_paths=active_path_strings,
            missing_paths=missing_paths,
            started_at=started_at,
        )
        if critical_missing_paths:
            message = (
                "Import aborted because required P-drive source folders are unavailable: "
                + "; ".join(critical_missing_paths)
            )
            store.finish_import_run(
                run_id,
                status="error",
                message=message,
                started_at=started_at,
                active_paths=active_path_strings,
                missing_paths=missing_paths,
                last_error=message,
                errors=1,
            )
            store.finish_import_monitor(status="error", message=message, last_error=message)
            store.record_import_result(
                status="error",
                trigger=trigger,
                message=message,
                processed=0,
                skipped=0,
                active_paths=active_path_strings,
                missing_paths=missing_paths,
                started_at=started_at,
            )
            raise AdminSettingsError(message)
        if not active_paths:
            message = "No active source folders are available. Save a valid test or production path first."
            store.finish_import_run(
                run_id,
                status="error",
                message=message,
                started_at=started_at,
                active_paths=[],
                missing_paths=missing_paths,
                last_error=message,
                errors=1,
            )
            store.finish_import_monitor(status="error", message=message, last_error=message)
            store.record_import_result(
                status="error",
                trigger=trigger,
                message=message,
                processed=0,
                skipped=0,
                active_paths=[],
                missing_paths=missing_paths,
                started_at=started_at,
            )
            raise AdminSettingsError(message)

        try:
            counts = import_paths(
                active_paths,
                changed_since=_normal_import_changed_since(),
                progress_callback=_progress_updater(store, trigger, active_path_strings, missing_paths),
                warning_callback=_warning_reporter(
                    store,
                    trigger=trigger,
                    started_at=started_at,
                    active_paths=active_path_strings,
                    missing_paths=missing_paths,
                ),
            )
        except Exception as exc:
            trace_text = traceback.format_exc()
            atomic_write_text(IMPORT_ERROR_LOG_PATH, trace_text, encoding="utf-8")
            store.save_error_report(
                category="import-error",
                summary=f"Import failed: {exc}",
                traceback_text=trace_text,
                extra={
                    "trigger": trigger,
                    "started_at": started_at,
                    "active_paths": active_path_strings,
                    "missing_paths": missing_paths,
                },
                force=True,
                state=state,
            )
            store.finish_import_run(
                run_id,
                status="error",
                message=f"Import failed: {exc}",
                started_at=started_at,
                active_paths=active_path_strings,
                missing_paths=missing_paths,
                last_error=trace_text,
                errors=1,
            )
            store.finish_import_monitor(
                status="error",
                message=f"Import failed: {exc}",
                last_error=trace_text,
            )
            store.record_import_result(
                status="error",
                trigger=trigger,
                message=str(exc),
                processed=0,
                skipped=0,
                active_paths=active_path_strings,
                missing_paths=missing_paths,
                started_at=started_at,
            )
            raise

        skipped_parts: list[str] = []
        if counts.get("unchanged_skipped"):
            skipped_parts.append(f"{counts['unchanged_skipped']} unchanged")
        if counts.get("duplicate_candidate_skipped"):
            skipped_parts.append(f"{counts['duplicate_candidate_skipped']} duplicate DAT candidates")
        if counts.get("missing_skipped"):
            skipped_parts.append(f"{counts['missing_skipped']} missing or unavailable")

        if counts["processed"] == 0 and counts["skipped"] == 0:
            message = "No new files found."
        elif skipped_parts:
            message = f"Imported {counts['processed']} files and skipped {counts['skipped']} files ({'; '.join(skipped_parts)})."
        else:
            message = f"Imported {counts['processed']} files."
        if counts.get("unstable_recent_files"):
            message = (
                f"{message} Deferred {counts['unstable_recent_files']} recently modified files until they remain unchanged for 2 minutes."
            )
        if missing_paths:
            message = f"{message} Missing folders: {'; '.join(missing_paths)}"

        store.finish_import_monitor(
            status="success",
            message=message,
            current_file="",
            current_step=counts["total_steps"],
            total_steps=counts["total_steps"],
            processed=counts["processed"],
            skipped=counts["skipped"],
            errors=counts["errors"],
            missing_files=counts["missing_files"],
            total_supported_files=counts["total_supported_files"],
            nest_files=counts["nest_files"],
            dat_files=counts["dat_files"],
            dat_groups=counts["dat_groups"],
            duplicate_dat_files=counts["duplicate_dat_files"],
            filtered_old_files=counts["filtered_old_files"],
            unstable_recent_files=counts["unstable_recent_files"],
            scanned_roots=len(active_path_strings),
            total_roots=len(active_path_strings),
            discovered_supported_files=counts["total_supported_files"],
        )
        store.finish_import_run(
            run_id,
            status="success",
            message=message,
            started_at=started_at,
            active_paths=active_path_strings,
            missing_paths=missing_paths,
            **counts,
        )

        updated = store.record_import_result(
            status="success",
            trigger=trigger,
            message=message,
            processed=counts["processed"],
            skipped=counts["skipped"],
            active_paths=active_path_strings,
            missing_paths=missing_paths,
            started_at=started_at,
        )
        return store.latest_import_result(updated)


def start_import_job(store: AdminSettingsStore, *, trigger: str) -> bool:
    global _JOB_THREAD
    with _MONITOR_LOCK:
        if _JOB_THREAD is not None and _JOB_THREAD.is_alive():
            return False

        def _runner() -> None:
            global _JOB_THREAD
            try:
                run_import_cycle(store, trigger=trigger)
            except Exception:
                pass
            finally:
                with _MONITOR_LOCK:
                    _JOB_THREAD = None

        _JOB_THREAD = threading.Thread(target=_runner, name=f"newtracker-import-{trigger}", daemon=True)
        _JOB_THREAD.start()
        return True


def clear_parsed_data() -> int:
    with get_connection() as connection:
        create_schema(connection)
        connection.executescript(
            """
            DELETE FROM scan_events;
            DELETE FROM monitor_unit_sources;
            DELETE FROM monitor_units;
            DELETE FROM flat_scan_items;
            DELETE FROM flat_scan_sessions;
            DELETE FROM forming_batch_items;
            DELETE FROM forming_batches;
            DELETE FROM nest_part_enrichment;
            DELETE FROM resolved_nest_parts;
            DELETE FROM nest_parts;
            DELETE FROM program_nests;
            DELETE FROM part_attributes;
            DELETE FROM job_orders;
            DELETE FROM job_labels;
            DELETE FROM job_parts;
            DELETE FROM job_folders;
            DELETE FROM missed_scans;
            DELETE FROM processed_files;
            DELETE FROM import_runs;
            DELETE FROM sqlite_sequence WHERE name IN (
                'scan_events',
                'monitor_unit_sources',
                'monitor_units',
                'flat_scan_items',
                'flat_scan_sessions',
                'forming_batch_items',
                'forming_batches',
                'nest_part_enrichment',
                'resolved_nest_parts',
                'nest_parts',
                'program_nests',
                'part_attributes',
                'job_orders',
                'job_labels',
                'job_parts',
                'job_folders',
                'missed_scans',
                'processed_files',
                'import_runs'
            );
            """
        )
        connection.commit()

    clear_scan_cache()
    UiStateStore.clear_all_persisted_state()
    return 1


def _is_auto_import_due(state: dict[str, Any]) -> bool:
    interval_minutes = int(state.get("poll_interval_minutes") or 0)
    if interval_minutes <= 0:
        return False

    completed_at = str(AdminSettingsStore().latest_import_result(state).get("completed_at", "")).strip()
    if not completed_at:
        return True

    try:
        last_run = datetime.fromisoformat(completed_at)
    except ValueError:
        return True
    return datetime.now() >= last_run + timedelta(minutes=interval_minutes)


def _monitor_loop() -> None:
    store = AdminSettingsStore()
    while True:
        try:
            state = store.read()
            if _is_auto_import_due(state):
                start_import_job(store, trigger="auto")
        except Exception:
            pass
        time.sleep(10)


def ensure_import_monitor_started() -> None:
    global _MONITOR_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_STARTED:
            return
        AdminSettingsStore().mark_interrupted_import_runs()
        thread = threading.Thread(target=_monitor_loop, name="newtracker-import-monitor", daemon=True)
        thread.start()
        _MONITOR_STARTED = True