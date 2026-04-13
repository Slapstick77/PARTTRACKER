from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from newtracker.db import DATA_DIR, DB_PATH
from newtracker.schema import create_schema


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as connection:
        create_schema(connection)
        connection.commit()

    print(f"Database initialized at: {DB_PATH}")


if __name__ == "__main__":
    main()
