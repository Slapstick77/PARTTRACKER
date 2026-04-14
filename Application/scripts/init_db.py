from __future__ import annotations

import sqlite3
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = APP_ROOT / "data"
DB_PATH = DATA_DIR / "newtracker.db"


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS program_nests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            barcode_filename TEXT NOT NULL UNIQUE,
            program_file_name TEXT NOT NULL,
            program_number TEXT,
            machine_type TEXT,
            sheet_program_name TEXT,
            material_code TEXT,
            sheet_length REAL,
            sheet_width REAL,
            program_date TEXT,
            program_time TEXT,
            process_count INTEGER,
            order_number_raw TEXT,
            order_process_code TEXT,
            build_date_code TEXT,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS nest_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nest_id INTEGER NOT NULL,
            part_number TEXT NOT NULL,
            part_revision TEXT,
            part_revision_key TEXT NOT NULL DEFAULT '',
            quantity_nested INTEGER NOT NULL DEFAULT 0,
            order_number_raw TEXT,
            npt_sequence TEXT,
            npt_quantity INTEGER,
            npt_rotation TEXT,
            npt_operation TEXT,
            npt_x REAL,
            npt_y REAL,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (nest_id) REFERENCES program_nests(id) ON DELETE CASCADE,
            UNIQUE (nest_id, part_number, part_revision_key)
        );

        CREATE TABLE IF NOT EXISTS part_attributes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            com_number TEXT,
            com_number_key TEXT NOT NULL DEFAULT '',
            part_number TEXT NOT NULL,
            rev_level TEXT,
            rev_level_key TEXT NOT NULL DEFAULT '',
            build_date TEXT,
            build_date_key TEXT NOT NULL DEFAULT '',
            quantity_per INTEGER,
            nested_on TEXT,
            length REAL,
            width REAL,
            thickness TEXT,
            item_class TEXT,
            department_number TEXT,
            part_parent TEXT,
            ops_files TEXT,
            pair_part_number TEXT,
            p4_edits TEXT,
            collection_cart TEXT,
            routing TEXT,
            model_number TEXT,
            shear TEXT,
            punch TEXT,
            form TEXT,
            requires_forming INTEGER NOT NULL DEFAULT 0,
            weight REAL,
            coded_part_msg TEXT,
            parent_model_number TEXT,
            skid_number TEXT,
            page_number TEXT,
            split_value TEXT,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (com_number_key, part_number, rev_level_key, build_date_key)
        );

        CREATE TABLE IF NOT EXISTS nest_part_enrichment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nest_part_id INTEGER NOT NULL,
            part_attribute_id INTEGER NOT NULL,
            match_method TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE CASCADE,
            FOREIGN KEY (part_attribute_id) REFERENCES part_attributes(id) ON DELETE CASCADE,
            UNIQUE (nest_part_id, part_attribute_id)
        );

        CREATE TABLE IF NOT EXISTS flat_scan_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nest_id INTEGER NOT NULL,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            FOREIGN KEY (nest_id) REFERENCES program_nests(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS flat_scan_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flat_scan_session_id INTEGER NOT NULL,
            nest_part_id INTEGER NOT NULL,
            expected_quantity INTEGER NOT NULL,
            scanned_quantity INTEGER NOT NULL DEFAULT 0,
            is_complete INTEGER NOT NULL DEFAULT 0,
            requires_forming INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (flat_scan_session_id) REFERENCES flat_scan_sessions(id) ON DELETE CASCADE,
            FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE CASCADE,
            UNIQUE (flat_scan_session_id, nest_part_id)
        );

        CREATE TABLE IF NOT EXISTS forming_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_code TEXT NOT NULL UNIQUE,
            source_nest_id INTEGER,
            com_number TEXT,
            build_date_code TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            completed_at TEXT,
            FOREIGN KEY (source_nest_id) REFERENCES program_nests(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS forming_batch_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            forming_batch_id INTEGER NOT NULL,
            nest_part_id INTEGER,
            part_attribute_id INTEGER,
            part_number TEXT NOT NULL,
            part_revision TEXT,
            expected_quantity INTEGER NOT NULL DEFAULT 0,
            scanned_quantity INTEGER NOT NULL DEFAULT 0,
            is_complete INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (forming_batch_id) REFERENCES forming_batches(id) ON DELETE CASCADE,
            FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE SET NULL,
            FOREIGN KEY (part_attribute_id) REFERENCES part_attributes(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS scan_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            barcode_value TEXT NOT NULL,
            part_number TEXT,
            part_revision TEXT,
            flat_scan_session_id INTEGER,
            forming_batch_id INTEGER,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (flat_scan_session_id) REFERENCES flat_scan_sessions(id) ON DELETE SET NULL,
            FOREIGN KEY (forming_batch_id) REFERENCES forming_batches(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_nest_parts_part_number
            ON nest_parts(part_number, part_revision);

        CREATE INDEX IF NOT EXISTS idx_part_attributes_lookup
            ON part_attributes(part_number, rev_level, build_date, com_number);

        CREATE INDEX IF NOT EXISTS idx_forming_batch_items_lookup
            ON forming_batch_items(part_number, part_revision);
        """
    )


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as connection:
        create_schema(connection)
        connection.commit()

    print(f"Database initialized at: {DB_PATH}")


if __name__ == "__main__":
    main()
