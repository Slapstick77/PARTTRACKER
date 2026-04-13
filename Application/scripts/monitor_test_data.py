from __future__ import annotations

"""Legacy standalone watcher.

The Flask admin auto-import now runs only inside the UI app process.
This script is not used by the app runtime.
"""

import sys
import time
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = WORKSPACE_ROOT / "Application" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from newtracker.importer import import_test_data

TEST_DATA_ROOT = WORKSPACE_ROOT / "TestData"
POLL_SECONDS = 5


def main() -> None:
    print(f"Monitoring: {TEST_DATA_ROOT}")
    while True:
        counts = import_test_data(TEST_DATA_ROOT)
        if counts["processed"]:
            print(f"Imported {counts['processed']} new/changed files; skipped {counts['skipped']} unchanged files")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
