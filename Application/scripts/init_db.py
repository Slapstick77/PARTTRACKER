from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from newtracker.db import DatabaseConfigurationError, describe_database_target, get_connection
from newtracker.schema import create_schema


def main() -> None:
    try:
        with get_connection() as connection:
            create_schema(connection)
            connection.commit()
    except DatabaseConfigurationError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Database initialized for: {describe_database_target()}")


if __name__ == "__main__":
    main()
