from __future__ import annotations

import sys
import traceback
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from flask import Flask


def _build_startup_error_app(startup_error: Exception) -> Flask:
    app = Flask(__name__)
    trace_text = "".join(
        traceback.format_exception(type(startup_error), startup_error, startup_error.__traceback__)
    )

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def startup_failure(path: str) -> tuple[str, int]:
        return (
            "<h1>Application startup error</h1>"
            "<p>NEWTRACKER failed before it could finish initializing the web app.</p>"
            f"<p><strong>Request path:</strong> {escape('/' + path if path else '/')}</p>"
            f"<pre>{escape(trace_text)}</pre>",
            500,
        )

    return app


try:
    from newtracker.ui_app import create_ui_app

    app = create_ui_app()
except Exception as startup_error:
    _trace = "".join(
        traceback.format_exception(type(startup_error), startup_error, startup_error.__traceback__)
    )
    print("NEWTRACKER STARTUP ERROR:\n" + _trace, file=sys.stderr, flush=True)
    app = _build_startup_error_app(startup_error)