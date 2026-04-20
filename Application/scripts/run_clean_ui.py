from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from newtracker.admin_settings import AdminSettingsStore
from newtracker.ui_app import create_ui_app

app = create_ui_app()


if __name__ == "__main__":
    app.run(debug=AdminSettingsStore().debug_enabled(), host="127.0.0.1", port=5000)
