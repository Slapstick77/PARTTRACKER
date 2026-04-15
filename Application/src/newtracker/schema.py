from __future__ import annotations

import sqlite3


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
            normalized_rev_key TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS job_folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_name TEXT NOT NULL,
            folder_path TEXT NOT NULL UNIQUE,
            com_number TEXT,
            build_date_code TEXT,
            project_name TEXT,
            source_root TEXT,
            last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS job_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_folder_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            part_number TEXT NOT NULL,
            revision TEXT,
            revision_key TEXT NOT NULL DEFAULT '',
            build_date_code TEXT,
            order_number_raw TEXT,
            process_code TEXT,
            routing TEXT,
            nested_on TEXT,
            quantity INTEGER,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_folder_id) REFERENCES job_folders(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS job_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_folder_id INTEGER NOT NULL,
            part_number TEXT NOT NULL,
            barcode TEXT,
            assembly TEXT,
            unit_id TEXT,
            build_day TEXT,
            nest_name TEXT,
            material TEXT,
            routing TEXT,
            quantity INTEGER,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_folder_id) REFERENCES job_folders(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS job_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_folder_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            part_number TEXT,
            material TEXT,
            profile TEXT,
            order_group TEXT,
            raw_identifier TEXT,
            quantity INTEGER,
            length REAL,
            source_file_path TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_folder_id) REFERENCES job_folders(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS resolved_nest_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nest_id INTEGER NOT NULL,
            nest_part_id INTEGER NOT NULL UNIQUE,
            barcode_filename TEXT NOT NULL,
            build_date_code TEXT,
            order_number_raw TEXT,
            part_number TEXT NOT NULL,
            part_revision TEXT,
            quantity_nested INTEGER NOT NULL DEFAULT 0,
            com_number TEXT,
            form_value TEXT,
            requires_forming INTEGER,
            nested_on TEXT,
            resolution_status TEXT NOT NULL,
            resolution_rule TEXT,
            match_candidate_count INTEGER NOT NULL DEFAULT 0,
            match_build_dates TEXT,
            match_com_numbers TEXT,
            matched_job_folder_id INTEGER,
            matched_job_part_id INTEGER,
            job_match_score INTEGER NOT NULL DEFAULT 0,
            evidence_summary TEXT,
            matched_part_attribute_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (nest_id) REFERENCES program_nests(id) ON DELETE CASCADE,
            FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE CASCADE,
            FOREIGN KEY (matched_job_folder_id) REFERENCES job_folders(id) ON DELETE SET NULL,
            FOREIGN KEY (matched_job_part_id) REFERENCES job_parts(id) ON DELETE SET NULL,
            FOREIGN KEY (matched_part_attribute_id) REFERENCES part_attributes(id) ON DELETE SET NULL
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

        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            modified_time REAL NOT NULL,
            content_hash TEXT,
            status TEXT NOT NULL,
            last_error TEXT,
            processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS import_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            active_paths_json TEXT NOT NULL DEFAULT '[]',
            missing_paths_json TEXT NOT NULL DEFAULT '[]',
            processed INTEGER NOT NULL DEFAULT 0,
            skipped INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            missing_files INTEGER NOT NULL DEFAULT 0,
            total_supported_files INTEGER NOT NULL DEFAULT 0,
            nest_files INTEGER NOT NULL DEFAULT 0,
            dat_files INTEGER NOT NULL DEFAULT 0,
            dat_groups INTEGER NOT NULL DEFAULT 0,
            duplicate_dat_files INTEGER NOT NULL DEFAULT 0,
            filtered_old_files INTEGER NOT NULL DEFAULT 0,
            unstable_recent_files INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_nest_parts_part_number
            ON nest_parts(part_number, part_revision);

        CREATE INDEX IF NOT EXISTS idx_part_attributes_lookup
            ON part_attributes(part_number, rev_level, build_date, com_number);

        CREATE INDEX IF NOT EXISTS idx_job_folders_lookup
            ON job_folders(com_number, build_date_code);

        CREATE INDEX IF NOT EXISTS idx_job_parts_lookup
            ON job_parts(part_number, revision_key, build_date_code, order_number_raw);

        CREATE INDEX IF NOT EXISTS idx_job_labels_lookup
            ON job_labels(part_number, unit_id, build_day);

        CREATE INDEX IF NOT EXISTS idx_job_orders_lookup
            ON job_orders(part_number, source_type, order_group);

        CREATE INDEX IF NOT EXISTS idx_forming_batch_items_lookup
            ON forming_batch_items(part_number, part_revision);

        CREATE INDEX IF NOT EXISTS idx_processed_files_status
            ON processed_files(status, file_type);

        CREATE INDEX IF NOT EXISTS idx_import_runs_started_at
            ON import_runs(started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_import_runs_status_started_at
            ON import_runs(status, started_at DESC);
        """
    )

    existing_part_attribute_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(part_attributes)").fetchall()
    }
    if "normalized_rev_key" not in existing_part_attribute_columns:
        connection.execute(
            "ALTER TABLE part_attributes ADD COLUMN normalized_rev_key TEXT NOT NULL DEFAULT ''"
        )

    existing_resolved_tables = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='resolved_nest_parts'"
    ).fetchone()
    if existing_resolved_tables is None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS resolved_nest_parts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nest_id INTEGER NOT NULL,
                nest_part_id INTEGER NOT NULL UNIQUE,
                barcode_filename TEXT NOT NULL,
                build_date_code TEXT,
                order_number_raw TEXT,
                part_number TEXT NOT NULL,
                part_revision TEXT,
                quantity_nested INTEGER NOT NULL DEFAULT 0,
                com_number TEXT,
                form_value TEXT,
                requires_forming INTEGER,
                nested_on TEXT,
                resolution_status TEXT NOT NULL,
                resolution_rule TEXT,
                match_candidate_count INTEGER NOT NULL DEFAULT 0,
                match_build_dates TEXT,
                match_com_numbers TEXT,
                matched_part_attribute_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (nest_id) REFERENCES program_nests(id) ON DELETE CASCADE,
                FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE CASCADE,
                FOREIGN KEY (matched_part_attribute_id) REFERENCES part_attributes(id) ON DELETE SET NULL
            );
            """
        )

    existing_resolved_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(resolved_nest_parts)").fetchall()
    }
    if "match_candidate_count" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN match_candidate_count INTEGER NOT NULL DEFAULT 0"
        )
    if "match_build_dates" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN match_build_dates TEXT"
        )
    if "match_com_numbers" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN match_com_numbers TEXT"
        )
    if "matched_job_folder_id" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN matched_job_folder_id INTEGER"
        )
    if "matched_job_part_id" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN matched_job_part_id INTEGER"
        )
    if "job_match_score" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN job_match_score INTEGER NOT NULL DEFAULT 0"
        )
    if "evidence_summary" not in existing_resolved_columns:
        connection.execute(
            "ALTER TABLE resolved_nest_parts ADD COLUMN evidence_summary TEXT"
        )

    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_part_attributes_resolve_lookup
            ON part_attributes(part_number, normalized_rev_key, build_date);

        CREATE INDEX IF NOT EXISTS idx_part_attributes_part_com_lookup
            ON part_attributes(part_number, com_number);

        CREATE INDEX IF NOT EXISTS idx_job_parts_resolve_lookup
            ON job_parts(part_number, revision_key, job_folder_id);

        CREATE INDEX IF NOT EXISTS idx_job_parts_job_folder_part_lookup
            ON job_parts(job_folder_id, part_number);

        CREATE INDEX IF NOT EXISTS idx_job_labels_job_folder_part_lookup
            ON job_labels(job_folder_id, part_number);

        CREATE INDEX IF NOT EXISTS idx_resolved_nest_parts_barcode
            ON resolved_nest_parts(barcode_filename, part_number);
        """
    )
