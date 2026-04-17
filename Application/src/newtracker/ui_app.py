from __future__ import annotations

import re
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from flask import Flask, flash, redirect, render_template, request, session, url_for
from markupsafe import Markup, escape

from .admin_settings import (
    AdminSettingsError,
    AdminSettingsStore,
    clear_parsed_data,
    ensure_import_monitor_started,
    start_import_job,
)
from .ui_state import UiStateError, UiStateStore

TEMPLATE_DIR = Path(__file__).resolve().parent / "ui" / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "ui" / "static"
CHANGELOG_PATH = Path(__file__).resolve().parents[3] / "CHANGELOG.md"
INLINE_CODE_PATTERN = re.compile(r"`([^`]+)`")


class SessionUiStateProxy:
    def __init__(self, resolver):
        self._resolver = resolver

    def __getattr__(self, name: str):
        return getattr(self._resolver(), name)


def _render_inline_changelog_text(text: str) -> Markup:
    rendered: list[Markup] = []
    last_index = 0
    for match in INLINE_CODE_PATTERN.finditer(text):
        rendered.append(Markup(escape(text[last_index:match.start()])))
        rendered.append(Markup("<code>%s</code>" % escape(match.group(1))))
        last_index = match.end()
    rendered.append(Markup(escape(text[last_index:])))
    return Markup("").join(rendered)


def _parse_changelog(raw_text: str) -> dict[str, Any]:
    intro: list[Markup] = []
    entries: list[dict[str, Any]] = []
    current_entry: dict[str, Any] | None = None
    current_section: dict[str, Any] | None = None

    for raw_line in raw_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            current_section = current_section if current_section and current_section["paragraphs"] else current_section
            continue
        if stripped == "# Changelog":
            continue
        if stripped.startswith("## "):
            current_entry = {
                "title": stripped[3:].strip(),
                "sections": [],
                "item_count": 0,
            }
            entries.append(current_entry)
            current_section = None
            continue
        if stripped.startswith("### "):
            if current_entry is None:
                continue
            current_section = {
                "title": stripped[4:].strip(),
                "items": [],
                "paragraphs": [],
            }
            current_entry["sections"].append(current_section)
            continue

        rendered = _render_inline_changelog_text(stripped)
        if stripped.startswith("- "):
            content = _render_inline_changelog_text(stripped[2:].strip())
            section = current_section
            if section is None:
                if current_entry is None:
                    intro.append(content)
                    continue
                section = {"title": "Notes", "items": [], "paragraphs": []}
                current_entry["sections"].append(section)
                current_section = section
            section["items"].append(content)
            if current_entry is not None:
                current_entry["item_count"] += 1
            continue

        if current_section is not None:
            current_section["paragraphs"].append(rendered)
        elif current_entry is None:
            intro.append(rendered)

    entries.reverse()
    return {
        "intro": intro,
        "entries": entries,
    }


def create_ui_app() -> Flask:
    app = Flask(__name__, template_folder=str(TEMPLATE_DIR), static_folder=str(STATIC_DIR))
    admin_store = AdminSettingsStore()
    app.config["SECRET_KEY"] = admin_store.secret_key()

    def current_ui_session_key() -> str:
        session_key = str(session.get("ui_session_key") or "").strip()
        if not session_key:
            session_key = secrets.token_urlsafe(16)
            session["ui_session_key"] = session_key
            session.modified = True
        return session_key

    def resolve_ui_store() -> Any:
        return cast(Any, UiStateStore(session_key=current_ui_session_key()))

    store = cast(Any, SessionUiStateProxy(resolve_ui_store))

    @app.before_request
    def start_background_monitor() -> None:
        ensure_import_monitor_started()
        current_ui_session_key()

    def build_context() -> dict:
        state = store.read()
        summary = store.summary(state)
        has_active_batch = bool(state.get("nest_data")) and summary["expected_total"] > 0
        return {
            "state": state,
            "summary": summary,
            "expected_parts": store.expected_remaining_list(state),
            "scanned_parts": store.scanned_counts(state),
            "has_active_batch": has_active_batch,
            "current_run_number": int(state.get("current_run_number") or 0),
            "repeat_scan_pending": bool(state.get("repeat_scan_pending")),
            "pending_repeat_dat": str(state.get("pending_repeat_dat") or ""),
            "pending_repeat_run_number": int(state.get("pending_repeat_run_number") or 0),
            "can_complete": (
                (not state.get("review_mode"))
                and state.get("flat_scan_status") != "completed"
                and summary["remaining_total"] == 0
                and summary["scanned_total"] > 0
            ),
            "can_force_complete": (not state.get("review_mode")) and bool(state.get("nest_data")) and summary["expected_total"] > 0,
            "can_edit_scanned": bool(state.get("nest_data")) and bool(state.get("scanned_parts")),
            "scan_edit_mode": bool(state.get("scan_edit_mode")),
            "review_mode": bool(state.get("review_mode")),
            "review_edit_mode": bool(state.get("review_edit_mode")),
            "review_rows": list(state.get("review_rows", [])),
            "review_missed_rows": list(state.get("review_missed_rows", [])),
        }

    def admin_context() -> dict:
        settings = admin_store.read()
        return {
            "settings": settings,
            "sources": admin_store.describe_sources(settings),
            "last_import": admin_store.latest_import_result(settings),
            "import_monitor": admin_store.import_monitor(),
            "admin_username": admin_store.admin_username(settings),
            "security": settings.get("security", {}),
        }

    def require_admin():
        if session.get("is_admin"):
            return None
        flash("Log in as admin to open settings.", "error")
        return redirect(url_for("admin_login"))

    def build_formed_context() -> dict:
        return store.formed_context()

    def build_monitor_context() -> dict:
        return store.monitor_context()

    def load_changelog() -> dict[str, str]:
        if not CHANGELOG_PATH.exists():
            return {
                "content": "# Changelog\n\nNo changelog entries yet.",
                "updated_at": "",
            }
        updated_at = datetime.fromtimestamp(CHANGELOG_PATH.stat().st_mtime).isoformat(timespec="seconds")
        return {
            "content": CHANGELOG_PATH.read_text(encoding="utf-8"),
            "updated_at": updated_at,
        }

    @app.get("/")
    def home():
        return render_template("index.html", **build_context())

    @app.post("/scan/<field_name>")
    def scan_field(field_name: str):
        try:
            state = store.scan_field(field_name, request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            if not (field_name == "nest_data" and state.get("repeat_scan_pending")):
                flash("Scan accepted.", "success")
        return redirect(url_for("home"))

    @app.post("/repeat-scan/confirm")
    def confirm_repeat_scan():
        try:
            state = store.confirm_repeat_scan()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(
                f"Started repeat run {int(state.get('current_run_number') or 0)} for {state.get('nest_data') or ''}.",
                "success",
            )
        return redirect(url_for("home"))

    @app.post("/repeat-scan/cancel")
    def cancel_repeat_scan():
        try:
            store.cancel_repeat_scan()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Repeat scan canceled.", "success")
        return redirect(url_for("home"))

    @app.get("/formed")
    def formed_home():
        return render_template("formed_scanner.html", **build_formed_context())

    @app.get("/monitor")
    def monitor_dashboard():
        return render_template("monitor_dashboard.html", **build_monitor_context())

    @app.post("/formed/scan-dat")
    def formed_scan_dat():
        try:
            store.formed_scan_dat(request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        return redirect(url_for("formed_home"))

    @app.post("/formed/scan-part")
    def formed_scan_part():
        try:
            store.formed_scan_part(request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        return redirect(url_for("formed_home"))

    @app.post("/formed/complete")
    def formed_complete_batch():
        try:
            updated_count = store.formed_complete_current_batch(request.form.get("batch_id", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(f"Updated {updated_count} tracker rows to Formed.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/force-complete")
    def formed_force_complete_batch():
        try:
            scanned_count, missing_count = store.formed_force_complete_current_batch(request.form.get("batch_id", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(
                f"Formed force complete updated {scanned_count} tracker rows to Formed and {missing_count} to Missing.",
                "success",
            )
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/edit")
    def formed_edit_review():
        try:
            store.start_formed_scan_edit(request.form.get("batch_id", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed scanned-part editing enabled for this browser session.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/add-manual")
    def formed_add_manual_review_row():
        flash("Manual formed rows are not supported in this flow.", "error")
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/save")
    def formed_save_review():
        try:
            store.save_formed_scan_edits(request.form)
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed scanned-part edits saved to this browser session.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/reset")
    def reset():
        store.reset()
        flash("Reset scan screen.", "success")
        return redirect(url_for("home"))

    @app.post("/complete")
    def complete_batch():
        try:
            updated_count = store.complete_current_batch()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(f"Updated {updated_count} tracker rows to Cut.", "success")
        return redirect(url_for("home"))

    @app.post("/force-complete")
    def force_complete_batch():
        try:
            scanned_count, missing_count = store.force_complete_current_batch()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(
                f"Force complete updated {scanned_count} tracker rows to Cut and {missing_count} to Missing.",
                "success",
            )
        return redirect(url_for("home"))

    @app.post("/review/edit")
    def edit_review():
        try:
            store.start_scan_edit()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Scanned part editing enabled for this browser session.", "success")
        return redirect(url_for("home"))

    @app.post("/review/add-manual")
    def add_manual_review_row():
        try:
            store.add_manual_review_row(request.form)
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Manual part row added.", "success")
        return redirect(url_for("home"))

    @app.post("/review/save")
    def save_review():
        try:
            store.save_scan_edits(request.form)
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Scanned part edits saved to this browser session.", "success")
        return redirect(url_for("home"))

    @app.post("/clear-completed")
    def clear_completed():
        removed = store.clear_completed_list()
        flash(f"Cleared part tracker ({removed} rows).", "success")
        return redirect(request.referrer or url_for("home"))

    @app.post("/clear-missed")
    def clear_missed():
        removed = store.clear_missed_list()
        flash(f"Cleared missed scans ({removed} rows).", "success")
        return redirect(url_for("completed_list"))

    @app.post("/clear-session-data")
    def clear_session_data():
        store.clear_session_data()
        flash("Session data cleared.", "success")
        return redirect(url_for("home"))

    @app.post("/clear-progress")
    def clear_progress():
        store.clear_development_progress()
        flash("Development progress cleared. Monitor, in-progress scan state, and archived lists were reset.", "success")
        return redirect(url_for("home"))

    @app.get("/completed-list")
    def completed_list():
        search_query = str(request.args.get("q", "") or "").strip()
        rows = store.get_completed_list(search_query)
        summary = {
            "total": len(rows),
            "prog": sum(1 for row in rows if row.get("stage") == "Prog"),
            "cut": sum(1 for row in rows if row.get("stage") == "Cut"),
            "formed": sum(1 for row in rows if row.get("stage") == "Formed"),
            "missing": sum(1 for row in rows if row.get("stage") == "Missing"),
        }
        return render_template(
            "completed_list.html",
            rows=rows,
            search_query=search_query,
            summary=summary,
        )

    @app.get("/completed-list/history")
    def completed_part_history():
        tracker_key = str(request.args.get("tracker_key", "") or "").strip()
        try:
            history = store.get_part_history(tracker_key)
        except UiStateError as exc:
            flash(str(exc), "error")
            return redirect(url_for("completed_list"))
        return render_template("completed_history.html", **history)

    @app.get("/api/state")
    def api_state():
        return build_context()

    @app.get("/api/formed-state")
    def api_formed_state():
        return build_formed_context()

    @app.get("/api/admin/import-status")
    def api_admin_import_status():
        guard = require_admin()
        if guard is not None:
            return {"error": "unauthorized"}, 401
        settings = admin_store.read()
        return {
            "import_monitor": admin_store.import_monitor(),
            "last_import": admin_store.latest_import_result(settings),
        }

    @app.get("/admin/login")
    def admin_login():
        if session.get("is_admin"):
            return redirect(url_for("admin_home"))
        return render_template("admin_login.html", admin_username=admin_store.admin_username())

    @app.post("/admin/login")
    def admin_login_submit():
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if admin_store.authenticate_admin(username, password):
            session["is_admin"] = True
            flash("Admin login successful.", "success")
            return redirect(url_for("admin_home"))

        flash("Invalid admin username or password.", "error")
        return redirect(url_for("admin_login"))

    @app.post("/admin/logout")
    def admin_logout():
        session.pop("is_admin", None)
        flash("Admin logged out.", "success")
        return redirect(url_for("home"))

    @app.get("/admin")
    def admin_home():
        guard = require_admin()
        if guard is not None:
            return guard
        return render_template("admin.html", **admin_context())

    @app.get("/admin/security")
    def admin_security():
        guard = require_admin()
        if guard is not None:
            return guard
        settings = admin_store.read()
        return render_template(
            "admin_security.html",
            admin_username=admin_store.admin_username(settings),
            security=settings.get("security", {}),
        )

    @app.get("/admin/changelog")
    def admin_changelog():
        guard = require_admin()
        if guard is not None:
            return guard
        changelog = load_changelog()
        parsed = _parse_changelog(changelog["content"])
        return render_template(
            "admin_changelog.html",
            changelog_intro=parsed["intro"],
            changelog_entries=parsed["entries"],
            changelog_updated=changelog["updated_at"],
        )

    @app.post("/admin/settings")
    def save_admin_settings():
        guard = require_admin()
        if guard is not None:
            return guard
        try:
            admin_store.update_from_form(request.form)
        except AdminSettingsError as exc:
            flash(str(exc), "error")
        else:
            flash("Admin settings saved.", "success")
        return redirect(url_for("admin_home"))

    @app.post("/admin/security")
    def save_admin_security():
        guard = require_admin()
        if guard is not None:
            return guard
        try:
            admin_store.update_security_from_form(request.form)
        except AdminSettingsError as exc:
            flash(str(exc), "error")
        else:
            flash("Admin credentials updated.", "success")
        return redirect(url_for("admin_security"))

    @app.post("/admin/import-now")
    def admin_import_now():
        guard = require_admin()
        if guard is not None:
            return guard
        started = start_import_job(admin_store, trigger="manual")
        if not started:
            flash("An import is already running. Watch the monitor below for progress.", "error")
        else:
            flash("Import started. The monitor will update while files are being processed.", "success")
        return redirect(url_for("admin_home"))

    @app.post("/admin/clear-parsed-data")
    def admin_clear_parsed_data():
        guard = require_admin()
        if guard is not None:
            return guard
        clear_parsed_data()
        flash("Parsed DAT and nest data cleared. Scan state and archives were reset.", "success")
        return redirect(url_for("admin_home"))

    return app
