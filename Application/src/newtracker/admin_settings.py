from __future__ import annotations

import json
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .db import APP_ROOT, get_connection
from .importer import clear_scan_cache, correction_import_paths, import_paths
from .schema import create_schema
from .ui_state import UiStateStore

ADMIN_SETTINGS_PATH = APP_ROOT / "data" / "admin_settings.json"
IMPORT_ERROR_LOG_PATH = APP_ROOT / "data" / "import_error.log"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "password"

DEFAULT_SOURCE_FOLDERS = {
    "amada": {
        "label": "Amada",
        "test_path": str(APP_ROOT.parent / "TestData" / "Amada"),
        "production_path": "",
        "use_production": False,
    },
    "emk1": {
        "label": "EMK1",
        "test_path": str(APP_ROOT.parent / "TestData" / "EMK1Test"),
        "production_path": "",
        "use_production": False,
    },
    "laser": {
        "label": "Laser",
        "test_path": str(APP_ROOT.parent / "TestData" / "Laser"),
        "production_path": "",
        "use_production": False,
    },
    "programming": {
        "label": "Programming Folders",
        "test_path": str(APP_ROOT.parent / "TestData" / "Programming folders"),
        "production_path": "",
        "use_production": False,
    },
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
            "scanned_roots": 0,
            "total_roots": 0,
            "discovered_supported_files": 0,
            "active_paths": [],
            "missing_paths": [],
            "last_error": "",
        }

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

    def _default_state(self) -> dict[str, Any]:
        return {
            "poll_interval_minutes": 0,
            "correction_schedule_time": "",
            "folders": {key: dict(value) for key, value in DEFAULT_SOURCE_FOLDERS.items()},
            "last_import": self._default_run_result("No import has been run yet."),
            "last_correction": self._default_run_result("No correction run has been run yet."),
        }

    def read(self) -> dict[str, Any]:
        with _SETTINGS_LOCK:
            state = self._default_state()
            if self.path.exists():
                saved = json.loads(self.path.read_text(encoding="utf-8"))
                state.update({key: value for key, value in saved.items() if key != "folders"})
                for folder_key, folder_value in saved.get("folders", {}).items():
                    if folder_key in state["folders"]:
                        state["folders"][folder_key].update(folder_value)
            return state

    def write(self, state: dict[str, Any]) -> None:
        with _SETTINGS_LOCK:
            persisted = {
                "poll_interval_minutes": state.get("poll_interval_minutes", 0),
                "correction_schedule_time": state.get("correction_schedule_time", ""),
                "folders": state.get("folders", {}),
                "last_import": state.get("last_import", self._default_state()["last_import"]),
                "last_correction": state.get("last_correction", self._default_state()["last_correction"]),
            }
            self.path.write_text(json.dumps(persisted, indent=2), encoding="utf-8")

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
            if not selected_path:
                missing_paths.append(f"{folder['label']}: no production folder set")
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

        correction_schedule_time = str(form.get("correction_schedule_time", "")).strip()
        if correction_schedule_time:
            try:
                datetime.strptime(correction_schedule_time, "%H:%M")
            except ValueError as exc:
                raise AdminSettingsError("Correction run time must use 24-hour HH:MM format.") from exc
        state["correction_schedule_time"] = correction_schedule_time

        for folder_key in state["folders"]:
            mode = str(form.get(f"source_mode_{folder_key}", "test")).strip().lower()
            production_path = str(form.get(f"production_path_{folder_key}", "")).strip()
            state["folders"][folder_key]["use_production"] = mode == "production"
            state["folders"][folder_key]["production_path"] = production_path

        self.write(state)
        return state

    def update_import_monitor(self, **updates: Any) -> dict[str, Any]:
        global _IMPORT_MONITOR_STATE
        with _SETTINGS_LOCK:
            monitor = dict(_IMPORT_MONITOR_STATE or self._default_import_monitor())
            monitor.update(updates)
            total_steps = int(monitor.get("total_steps") or 0)
            current_step = int(monitor.get("current_step") or 0)
            if total_steps > 0:
                progress_percent = min(100, max(0, int((current_step / total_steps) * 100)))
            else:
                progress_percent = 0 if monitor.get("status") == "idle" else int(monitor.get("progress_percent") or 0)
            monitor["progress_percent"] = progress_percent
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
        if not trigger.startswith("correction"):
            state["last_import"] = result
        if trigger.startswith("correction"):
            state["last_correction"] = dict(result)
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
            return dict(_IMPORT_MONITOR_STATE or self._default_import_monitor())


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
            scanned_roots=snapshot.get("scanned_roots", current.get("scanned_roots", 0)),
            total_roots=snapshot.get("total_roots", current.get("total_roots", len(active_path_strings))),
            discovered_supported_files=snapshot.get("discovered_supported_files", current.get("discovered_supported_files", 0)),
            active_paths=active_path_strings,
            missing_paths=missing_paths,
        )

    return _update


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


def run_import_cycle(store: AdminSettingsStore, *, trigger: str, correction_run: bool = False) -> dict[str, Any]:
    started_at = datetime.now().isoformat(timespec="seconds")
    with _IMPORT_LOCK:
        state = store.read()
        active_paths, missing_paths = store.get_active_paths(state)
        active_path_strings = [str(path) for path in active_paths]
        store.start_import_monitor(
            trigger=trigger,
            active_paths=active_path_strings,
            missing_paths=missing_paths,
            started_at=started_at,
        )
        if not active_paths:
            message = "No active source folders are available. Save a valid test or production path first."
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
            if correction_run:
                counts = correction_import_paths(
                    active_paths,
                    progress_callback=_progress_updater(store, trigger, active_path_strings, missing_paths),
                )
            else:
                counts = import_paths(
                    active_paths,
                    changed_since=_normal_import_changed_since(),
                    progress_callback=_progress_updater(store, trigger, active_path_strings, missing_paths),
                )
        except Exception as exc:
            trace_text = traceback.format_exc()
            IMPORT_ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            IMPORT_ERROR_LOG_PATH.write_text(trace_text, encoding="utf-8")
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

        message = f"Imported {counts['processed']} files and skipped {counts['skipped']} unchanged files."
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
            scanned_roots=len(active_path_strings),
            total_roots=len(active_path_strings),
            discovered_supported_files=counts["total_supported_files"],
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
        return updated["last_import"]


def start_import_job(store: AdminSettingsStore, *, trigger: str, correction_run: bool | None = None) -> bool:
    global _JOB_THREAD
    with _MONITOR_LOCK:
        if _JOB_THREAD is not None and _JOB_THREAD.is_alive():
            return False

        resolved_correction_run = trigger.startswith("correction") if correction_run is None else correction_run

        def _runner() -> None:
            global _JOB_THREAD
            try:
                run_import_cycle(store, trigger=trigger, correction_run=resolved_correction_run)
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
            DELETE FROM flat_scan_items;
            DELETE FROM flat_scan_sessions;
            DELETE FROM forming_batch_items;
            DELETE FROM forming_batches;
            DELETE FROM nest_part_enrichment;
            DELETE FROM resolved_nest_parts;
            DELETE FROM nest_parts;
            DELETE FROM program_nests;
            DELETE FROM part_attributes;
            DELETE FROM missed_scans;
            DELETE FROM processed_files;
            DELETE FROM sqlite_sequence WHERE name IN (
                'scan_events',
                'flat_scan_items',
                'flat_scan_sessions',
                'forming_batch_items',
                'forming_batches',
                'nest_part_enrichment',
                'resolved_nest_parts',
                'nest_parts',
                'program_nests',
                'part_attributes',
                'missed_scans',
                'processed_files'
            );
            """
        )
        connection.commit()

    clear_scan_cache()
    ui_state = UiStateStore()
    ui_state.clear_runtime_data()
    return 1


def _is_auto_import_due(state: dict[str, Any]) -> bool:
    interval_minutes = int(state.get("poll_interval_minutes") or 0)
    if interval_minutes <= 0:
        return False

    completed_at = str(state.get("last_import", {}).get("completed_at", "")).strip()
    if not completed_at:
        return True

    try:
        last_run = datetime.fromisoformat(completed_at)
    except ValueError:
        return True
    return datetime.now() >= last_run + timedelta(minutes=interval_minutes)


def _is_correction_due(state: dict[str, Any]) -> bool:
    correction_time = str(state.get("correction_schedule_time") or "").strip()
    if not correction_time:
        return False

    try:
        scheduled = datetime.strptime(correction_time, "%H:%M").time()
    except ValueError:
        return False

    now = datetime.now()
    scheduled_today = now.replace(hour=scheduled.hour, minute=scheduled.minute, second=0, microsecond=0)
    if now < scheduled_today:
        return False

    completed_at = str(state.get("last_correction", {}).get("completed_at", "")).strip()
    if not completed_at:
        return True

    try:
        last_run = datetime.fromisoformat(completed_at)
    except ValueError:
        return True
    return last_run.date() < now.date()


def _monitor_loop() -> None:
    store = AdminSettingsStore()
    while True:
        try:
            state = store.read()
            if _is_correction_due(state):
                start_import_job(store, trigger="correction-auto", correction_run=True)
            elif _is_auto_import_due(state):
                start_import_job(store, trigger="auto")
        except Exception:
            pass
        time.sleep(10)


def ensure_import_monitor_started() -> None:
    global _MONITOR_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_STARTED:
            return
        thread = threading.Thread(target=_monitor_loop, name="newtracker-import-monitor", daemon=True)
        thread.start()
        _MONITOR_STARTED = True