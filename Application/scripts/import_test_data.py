from __future__ import annotations

import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = WORKSPACE_ROOT / "Application" / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from newtracker.importer import import_test_data

TEST_DATA_ROOT = WORKSPACE_ROOT / "TestData"


def main() -> None:
    counts = import_test_data(TEST_DATA_ROOT)
    print(f"Processed files: {counts['processed']}")
    print(f"Skipped files: {counts['skipped']}")


if __name__ == "__main__":
    main()
