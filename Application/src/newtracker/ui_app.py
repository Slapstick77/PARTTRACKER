from __future__ import annotations

import base64
import json
import os
import re
import secrets
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from flask import Flask, flash, g, got_request_exception, redirect, render_template, request, send_file, session, url_for
from markupsafe import Markup, escape
from werkzeug.exceptions import HTTPException

from .admin_settings import (
    AdminSettingsError,
    AdminSettingsStore,
    MAIN_SCANNER_AUTO_MODE_AUTO_COMPLETE,
    MAIN_SCANNER_AUTO_MODE_FULL_AUTO,
    MAIN_SCANNER_AUTO_MODE_OFF,
    clear_parsed_data,
    ensure_import_monitor_started,
    start_import_job,
)
from .db import DatabaseConfigurationError, describe_database_target
from .error_reports import list_error_reports, resolve_error_report_path
from .ui_state import UiStateError, UiStateStore

TEMPLATE_DIR = Path(__file__).resolve().parent / "ui" / "templates"
STATIC_DIR = Path(__file__).resolve().parent / "ui" / "static"
CHANGELOG_PATH = Path(__file__).resolve().parents[3] / "CHANGELOG.md"
INLINE_CODE_PATTERN = re.compile(r"`([^`]+)`")

TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in TRUTHY_VALUES


def _normalize_domain_list(raw_value: str) -> set[str]:
    values = {value.strip().lower() for value in raw_value.split(",") if value.strip()}
    return values or {"jci.com"}


def _decode_ms_client_principal(raw_value: str) -> dict[str, Any]:
    padded = raw_value + "=" * (-len(raw_value) % 4)
    decoded = base64.b64decode(padded).decode("utf-8")
    parsed = json.loads(decoded)
    return parsed if isinstance(parsed, dict) else {}


def _extract_easy_auth_identity() -> tuple[str, str]:
    principal_name = str(request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME") or "").strip()
    tenant_id = ""

    principal_payload = str(request.headers.get("X-MS-CLIENT-PRINCIPAL") or "").strip()
    if principal_payload:
        try:
            parsed = _decode_ms_client_principal(principal_payload)
            claims = parsed.get("claims", [])
            if isinstance(claims, list):
                for claim in claims:
                    if not isinstance(claim, dict):
                        continue
                    claim_type = str(claim.get("typ") or "").lower()
                    claim_value = str(claim.get("val") or "").strip()
                    if not claim_value:
                        continue
                    if not principal_name and claim_type in {
                        "preferred_username",
                        "upn",
                        "email",
                        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
                        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
                    }:
                        principal_name = claim_value
                    if not tenant_id and claim_type in {
                        "tid",
                        "tenantid",
                        "http://schemas.microsoft.com/identity/claims/tenantid",
                    }:
                        tenant_id = claim_value
        except Exception:
            pass

    return principal_name, tenant_id


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
            g.created_ui_session_key = True
        elif not hasattr(g, "created_ui_session_key"):
            g.created_ui_session_key = False
        return session_key

    def resolve_ui_store() -> Any:
        return cast(Any, UiStateStore(session_key=current_ui_session_key()))

    store = cast(Any, SessionUiStateProxy(resolve_ui_store))

    def debug_reports_context(settings: dict[str, Any] | None = None) -> dict[str, Any]:
        current = settings or admin_store.read()
        report_directory = admin_store.error_report_directory(current)
        return {
            "enabled": admin_store.debug_enabled(current),
            "folder": str(report_directory),
            "folder_exists": report_directory.exists() and report_directory.is_dir(),
            "recent_reports": list_error_reports(report_directory),
        }

    def save_debug_report(
        *,
        category: str,
        summary: str,
        traceback_text: str,
        request_info: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Path | None:
        return admin_store.save_error_report(
            category=category,
            summary=summary,
            traceback_text=traceback_text,
            request_info=request_info,
            extra=extra,
            force=False,
        )

    @got_request_exception.connect_via(app)
    def capture_request_exception(_sender: Flask, exception: Exception, **_extra: Any) -> None:
        if isinstance(exception, HTTPException):
            return

        trace_text = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        request_payload = {
            "method": request.method,
            "path": request.path,
            "endpoint": request.endpoint,
            "query": {key: request.args.getlist(key) for key in request.args},
            "form": {key: request.form.getlist(key) for key in request.form},
            "json": request.get_json(silent=True) if request.is_json else None,
            "remote_addr": request.remote_addr,
            "user_agent": request.user_agent.string,
            "is_admin": bool(session.get("is_admin")),
        }
        save_debug_report(
            category="request-error",
            summary=f"{request.method} {request.path} failed: {exception}",
            traceback_text=trace_text,
            request_info=request_payload,
        )

    @app.errorhandler(DatabaseConfigurationError)
    def handle_database_configuration_error(exception: DatabaseConfigurationError):
        try:
            target = describe_database_target()
        except Exception:
            target = "unavailable"
        return (
            "<h1>Database configuration error</h1>"
            "<p>NEWTRACKER could not open its configured database connection.</p>"
            f"<p><strong>Target:</strong> {escape(target)}</p>"
            f"<p><strong>Details:</strong> {escape(str(exception))}</p>"
            "<p>Check the Azure App Service Application Settings and Azure SQL firewall access, then restart the app.</p>",
            500,
        )

    @app.errorhandler(Exception)
    def handle_unhandled_exception(exception: Exception):
        import sys
        trace_text = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )
        print("NEWTRACKER UNHANDLED EXCEPTION:\n" + trace_text, file=sys.stderr, flush=True)
        return (
            "<h1>Unhandled application error</h1>"
            f"<p><strong>Type:</strong> {escape(type(exception).__name__)}</p>"
            f"<p><strong>Details:</strong> {escape(str(exception))}</p>"
            f"<pre>{escape(trace_text)}</pre>",
            500,
        )

    @app.before_request
    def enforce_jci_identity() -> tuple[str, int] | None:
        if not _is_truthy(os.getenv("NEWTRACKER_REQUIRE_JCI_AUTH")):
            return None
        if request.path.startswith("/.auth"):
            return None
        if request.endpoint == "static":
            return None

        allowed_domains = _normalize_domain_list(os.getenv("NEWTRACKER_ALLOWED_EMAIL_DOMAINS", "jci.com"))
        allowed_tenant = str(os.getenv("NEWTRACKER_ALLOWED_TENANT_ID") or "").strip().lower()
        principal_name, tenant_id = _extract_easy_auth_identity()
        email = principal_name.strip().lower()
        if not email or "@" not in email:
            return (
                "<h1>Sign-in required</h1>"
                "<p>Please sign in with your JCI account to access NEWTRACKER.</p>",
                401,
            )

        domain = email.rsplit("@", 1)[-1]
        if domain not in allowed_domains:
            return (
                "<h1>Access denied</h1>"
                f"<p>The account {escape(email)} is not in an allowed domain.</p>",
                403,
            )

        if allowed_tenant and tenant_id.strip().lower() != allowed_tenant:
            return (
                "<h1>Access denied</h1>"
                "<p>Your account is not from the allowed Azure AD tenant.</p>",
                403,
            )
        g.easyauth_principal = email

    @app.before_request
    def start_background_monitor() -> None:
        ensure_import_monitor_started()
        current_ui_session_key()

    def maybe_auto_resume_main_session() -> dict[str, Any] | None:
        if not bool(getattr(g, "created_ui_session_key", False)):
            return None

        current_session = current_ui_session_key()
        current_state = UiStateStore(session_key=current_session).read()
        has_visible_session_state = bool(current_state.get("nest_data")) or bool(current_state.get("expected_parts")) or bool(current_state.get("scanned_parts")) or bool(current_state.get("repeat_scan_pending"))
        if has_visible_session_state:
            return None

        candidates = UiStateStore.list_resumable_sessions(
            current_session_key=current_session,
            limit=1,
            require_scanned_progress=True,
        )
        if not candidates:
            return None

        session["ui_session_key"] = candidates[0]["session_key"]
        session.modified = True
        return candidates[0]

    def build_context() -> dict:
        maybe_auto_resume_main_session()
        session_key = current_ui_session_key()
        state = store.read()
        summary = store.summary(state)
        has_active_batch = bool(state.get("nest_data")) and summary["expected_total"] > 0
        has_visible_session_state = bool(state.get("nest_data")) or bool(state.get("expected_parts")) or bool(state.get("scanned_parts"))
        resume_candidates = [] if has_visible_session_state else UiStateStore.list_resumable_sessions(current_session_key=session_key, require_scanned_progress=True)
        return {
            "state": state,
            "summary": summary,
            "expected_parts": store.expected_remaining_list(state),
            "scanned_parts": store.scanned_counts(state),
            "resume_candidates": resume_candidates,
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
            "extra_sources": admin_store.describe_extra_sources(settings),
            "debug_reports": debug_reports_context(settings),
            "last_import": admin_store.latest_import_result(settings),
            "import_monitor": admin_store.import_monitor(),
            "admin_username": admin_store.admin_username(settings),
            "security": settings.get("security", {}),
        }

    def maybe_apply_main_scanner_auto_mode(state: dict[str, Any]) -> tuple[str, str] | None:
        mode = admin_store.scanner_auto_mode()
        if mode == MAIN_SCANNER_AUTO_MODE_OFF:
            return None
        if state.get("repeat_scan_pending") or not state.get("nest_data"):
            return None

        dat_name = str(state.get("nest_data") or "").strip().upper()
        if mode == MAIN_SCANNER_AUTO_MODE_AUTO_COMPLETE:
            _, moved_count = store.auto_fill_current_batch()
            if moved_count <= 0:
                return None
            return (
                f"Auto Complete moved {moved_count} parts into Scanned for {dat_name}. Click Complete when ready.",
                "success",
            )

        _, moved_count = store.auto_fill_current_batch()
        updated_count = store.complete_current_batch()
        if moved_count > 0:
            return (
                f"Full Auto moved {moved_count} parts and completed {dat_name}. Updated {updated_count} parts to Cut.",
                "success",
            )
        return (f"Full Auto completed {dat_name}. Updated {updated_count} parts to Cut.", "success")

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

    @app.post("/resume-session")
    def resume_session():
        requested_session_key = str(request.form.get("session_key") or "").strip()
        available_sessions = {
            candidate["session_key"]: candidate
            for candidate in UiStateStore.list_resumable_sessions(current_session_key=current_ui_session_key(), limit=25)
        }
        selected = available_sessions.get(requested_session_key)
        if selected is None:
            flash("That saved scan session is no longer available.", "error")
            return redirect(url_for("home"))

        session["ui_session_key"] = requested_session_key
        session.modified = True
        flash(
            f"Resumed {selected['dat_name']} run {int(selected['run_number'] or 0) or 1}.",
            "success",
        )
        return redirect(url_for("home"))

    @app.post("/scan/<field_name>")
    def scan_field(field_name: str):
        try:
            state = store.scan_field(field_name, request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_scan(str(exc))
            flash(str(exc), "error")
        else:
            auto_notice = None
            if field_name == "nest_data" and not state.get("repeat_scan_pending"):
                auto_notice = maybe_apply_main_scanner_auto_mode(state)
            if auto_notice is not None:
                flash(auto_notice[0], auto_notice[1])
            elif not (field_name == "nest_data" and state.get("repeat_scan_pending")):
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
            auto_notice = maybe_apply_main_scanner_auto_mode(state)
            if auto_notice is not None:
                flash(auto_notice[0], auto_notice[1])
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

    @app.post("/formed/scan")
    def formed_scan():
        try:
            store.formed_scan_value(request.form.get("value", ""))
        except UiStateError as exc:
            store.invalidate_formed_scan(str(exc))
            flash(str(exc), "error")
        return redirect(url_for("formed_home"))

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

    @app.post("/admin/clear-scan-cache")
    def admin_clear_scan_cache():
        guard = require_admin()
        if guard is not None:
            return guard
        from .importer import clear_scan_cache
        clear_scan_cache()
        flash("Scan cache cleared. All files will be re-evaluated on the next import.", "success")
        return redirect(url_for("admin_home"))

    @app.post("/admin/clear-parsed-data")
    def admin_clear_parsed_data():
        guard = require_admin()
        if guard is not None:
            return guard
        clear_parsed_data()
        flash("Parsed DAT and nest data cleared. Scan state and archives were reset.", "success")
        return redirect(url_for("admin_home"))

    @app.post("/admin/error-reports/test")
    def admin_send_test_error_report():
        guard = require_admin()
        if guard is not None:
            return guard
        try:
            report_path = admin_store.save_error_report(
                category="test-log",
                summary="Manual test log from Admin Settings.",
                traceback_text="This is a manual NEWTRACKER test log created to verify the configured report folder is writable.",
                extra={
                    "created_from": "admin",
                    "request_path": request.path,
                },
                force=True,
                raise_on_error=True,
            )
        except OSError as exc:
            flash(f"Test log write failed: {exc}", "error")
        else:
            report_name = report_path.name if report_path is not None else "test log"
            flash(f"Test log saved: {report_name}", "success")
        return redirect(url_for("admin_home"))

    @app.get("/admin/error-reports/<path:report_name>")
    def admin_download_error_report(report_name: str):
        guard = require_admin()
        if guard is not None:
            return guard
        settings = admin_store.read()
        try:
            report_path = resolve_error_report_path(admin_store.error_report_directory(settings), report_name)
        except FileNotFoundError:
            flash("Error report file not found.", "error")
            return redirect(url_for("admin_home"))
        return send_file(report_path, as_attachment=True, download_name=report_path.name)

    return app
