from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from flask import Flask, flash, redirect, render_template, request, session, url_for

from .admin_settings import (
    ADMIN_PASSWORD,
    ADMIN_USERNAME,
    AdminSettingsError,
    AdminSettingsStore,
    clear_parsed_data,
    ensure_import_monitor_started,
    start_import_job,
)
from .ui_state import UiStateError, UiStateStore

TEMPLATE_DIR = Path(__file__).resolve().parent / "ui" / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "ui" / "static"


def create_ui_app() -> Flask:
    app = Flask(__name__, template_folder=str(TEMPLATE_DIR), static_folder=str(STATIC_DIR))
    app.config["SECRET_KEY"] = "newtracker-clean-ui"
    store = cast(Any, UiStateStore())
    admin_store = AdminSettingsStore()

    @app.before_request
    def start_background_monitor() -> None:
        ensure_import_monitor_started()

    def build_context() -> dict:
        state = store.read()
        summary = store.summary(state)
        return {
            "state": state,
            "summary": summary,
            "expected_parts": store.expected_remaining_list(state),
            "scanned_parts": store.scanned_counts(state),
            "can_complete": (not state.get("review_mode")) and summary["remaining_total"] == 0 and summary["scanned_total"] > 0,
            "can_force_complete": (not state.get("review_mode")) and bool(state.get("nest_data")) and summary["expected_total"] > 0,
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
            "last_import": settings.get("last_import", {}),
            "import_monitor": admin_store.import_monitor(),
            "admin_username": ADMIN_USERNAME,
        }

    def require_admin():
        if session.get("is_admin"):
            return None
        flash("Log in as admin to open settings.", "error")
        return redirect(url_for("admin_login"))

    def build_formed_context() -> dict:
        return store.formed_context()

    @app.get("/")
    def home():
        return render_template("index.html", **build_context())

    @app.post("/scan/<field_name>")
    def scan_field(field_name: str):
        try:
            store.scan_field(field_name, request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Scan accepted.", "success")
        return redirect(url_for("home"))

    @app.get("/formed")
    def formed_home():
        return render_template("formed_scanner.html", **build_formed_context())

    @app.post("/formed/scan-dat")
    def formed_scan_dat():
        try:
            store.formed_scan_dat(request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("DAT token accepted for formed scanning.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/scan-part")
    def formed_scan_part():
        try:
            store.formed_scan_part(request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed part accepted.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/complete")
    def formed_complete_batch():
        try:
            store.formed_complete_current_batch()
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed batch ready for review. Edit if needed, then save and send.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/force-complete")
    def formed_force_complete_batch():
        try:
            store.formed_force_complete_current_batch()
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed force complete opened review with missed scans included.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/edit")
    def formed_edit_review():
        try:
            store.enable_formed_review_edit(request.form)
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Formed review editing enabled.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/add-manual")
    def formed_add_manual_review_row():
        try:
            store.add_manual_formed_review_row(request.form)
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Manual formed row added.", "success")
        return redirect(url_for("formed_home"))

    @app.post("/formed/review/save")
    def formed_save_review():
        try:
            completed_count, missed_count = store.save_formed_review(request.form)
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(
                f"Sent {completed_count} formed rows to part tracker and {missed_count} to missed scans.",
                "success",
            )
        return redirect(url_for("formed_home"))

    @app.post("/reset")
    def reset():
        store.reset()
        flash("Reset scan screen.", "success")
        return redirect(url_for("home"))

    @app.post("/complete")
    def complete_batch():
        try:
            store.complete_current_batch()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Batch ready for review. Edit if needed, then save and send.", "success")
        return redirect(url_for("home"))

    @app.post("/force-complete")
    def force_complete_batch():
        try:
            store.force_complete_current_batch()
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Force complete opened review with missed scans included.", "success")
        return redirect(url_for("home"))

    @app.post("/review/edit")
    def edit_review():
        try:
            store.enable_review_edit(request.form)
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash("Review editing enabled.", "success")
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
            completed_count, missed_count = store.save_review(request.form)
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            flash(
                f"Sent {completed_count} rows to part tracker and {missed_count} rows to missed scans.",
                "success",
            )
        return redirect(url_for("home"))

    @app.post("/clear-completed")
    def clear_completed():
        removed = store.clear_completed_list()
        flash(f"Cleared archived list ({removed} rows).", "success")
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

    @app.get("/completed-list")
    def completed_list():
        rows = store.get_completed_list()
        missed_rows = store.get_missed_list()
        return render_template("completed_list.html", rows=rows, missed_rows=missed_rows)

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
            "last_import": settings.get("last_import", {}),
        }

    @app.get("/admin/login")
    def admin_login():
        if session.get("is_admin"):
            return redirect(url_for("admin_home"))
        return render_template("admin_login.html")

    @app.post("/admin/login")
    def admin_login_submit():
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
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
