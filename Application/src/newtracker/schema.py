from __future__ import annotations

import sqlite3
from typing import Any

from .db import get_database_settings


def _schema_backend() -> str:
    try:
        return get_database_settings().backend
    except Exception:
        return "sqlite"


def _execute_sqlserver_statements(connection: Any, statements: list[str]) -> None:
    for statement in statements:
        connection.execute(statement)


def _create_schema_sqlserver(connection: Any) -> None:
    create_statements = [
        """
        IF OBJECT_ID(N'dbo.program_nests', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.program_nests (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                barcode_filename NVARCHAR(255) NOT NULL UNIQUE,
                program_file_name NVARCHAR(255) NOT NULL,
                program_number NVARCHAR(255) NULL,
                machine_type NVARCHAR(255) NULL,
                sheet_program_name NVARCHAR(255) NULL,
                material_code NVARCHAR(255) NULL,
                sheet_length FLOAT NULL,
                sheet_width FLOAT NULL,
                program_date NVARCHAR(255) NULL,
                program_time NVARCHAR(255) NULL,
                process_count INT NULL,
                order_number_raw NVARCHAR(255) NULL,
                order_process_code NVARCHAR(255) NULL,
                build_date_code NVARCHAR(255) NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.nest_parts', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.nest_parts (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                nest_id INT NOT NULL,
                part_number NVARCHAR(255) NOT NULL,
                part_revision NVARCHAR(255) NULL,
                part_revision_key NVARCHAR(64) NOT NULL DEFAULT '',
                quantity_nested INT NOT NULL DEFAULT 0,
                order_number_raw NVARCHAR(255) NULL,
                npt_sequence NVARCHAR(255) NULL,
                npt_quantity INT NULL,
                npt_rotation NVARCHAR(255) NULL,
                npt_operation NVARCHAR(255) NULL,
                npt_x FLOAT NULL,
                npt_y FLOAT NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT UQ_nest_parts_nest_part UNIQUE (nest_id, part_number, part_revision_key),
                CONSTRAINT FK_nest_parts_program_nests FOREIGN KEY (nest_id) REFERENCES dbo.program_nests(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.part_attributes', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.part_attributes (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                com_number NVARCHAR(255) NULL,
                com_number_key NVARCHAR(64) NOT NULL DEFAULT '',
                part_number NVARCHAR(255) NOT NULL,
                rev_level NVARCHAR(255) NULL,
                rev_level_key NVARCHAR(64) NOT NULL DEFAULT '',
                normalized_rev_key NVARCHAR(64) NOT NULL DEFAULT '',
                build_date NVARCHAR(255) NULL,
                build_date_key NVARCHAR(64) NOT NULL DEFAULT '',
                quantity_per INT NULL,
                nested_on NVARCHAR(255) NULL,
                length FLOAT NULL,
                width FLOAT NULL,
                thickness NVARCHAR(255) NULL,
                item_class NVARCHAR(255) NULL,
                department_number NVARCHAR(255) NULL,
                part_parent NVARCHAR(255) NULL,
                ops_files NVARCHAR(MAX) NULL,
                pair_part_number NVARCHAR(255) NULL,
                p4_edits NVARCHAR(MAX) NULL,
                collection_cart NVARCHAR(255) NULL,
                routing NVARCHAR(MAX) NULL,
                model_number NVARCHAR(255) NULL,
                shear NVARCHAR(255) NULL,
                punch NVARCHAR(255) NULL,
                form NVARCHAR(255) NULL,
                requires_forming INT NOT NULL DEFAULT 0,
                weight FLOAT NULL,
                coded_part_msg NVARCHAR(MAX) NULL,
                parent_model_number NVARCHAR(255) NULL,
                skid_number NVARCHAR(255) NULL,
                page_number NVARCHAR(255) NULL,
                split_value NVARCHAR(255) NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT UQ_part_attributes_lookup UNIQUE (com_number_key, part_number, rev_level_key, build_date_key)
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.job_folders', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.job_folders (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                folder_name NVARCHAR(255) NOT NULL,
                folder_path NVARCHAR(450) NOT NULL UNIQUE,
                com_number NVARCHAR(255) NULL,
                build_date_code NVARCHAR(255) NULL,
                project_name NVARCHAR(255) NULL,
                source_root NVARCHAR(MAX) NULL,
                last_seen_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.job_parts', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.job_parts (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                job_folder_id INT NOT NULL,
                source_type NVARCHAR(255) NOT NULL,
                part_number NVARCHAR(255) NOT NULL,
                revision NVARCHAR(255) NULL,
                revision_key NVARCHAR(64) NOT NULL DEFAULT '',
                build_date_code NVARCHAR(255) NULL,
                order_number_raw NVARCHAR(255) NULL,
                process_code NVARCHAR(255) NULL,
                routing NVARCHAR(MAX) NULL,
                nested_on NVARCHAR(255) NULL,
                quantity INT NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_job_parts_job_folders FOREIGN KEY (job_folder_id) REFERENCES dbo.job_folders(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.job_labels', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.job_labels (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                job_folder_id INT NOT NULL,
                part_number NVARCHAR(255) NOT NULL,
                barcode NVARCHAR(255) NULL,
                assembly NVARCHAR(255) NULL,
                unit_id NVARCHAR(255) NULL,
                build_day NVARCHAR(255) NULL,
                nest_name NVARCHAR(255) NULL,
                material NVARCHAR(255) NULL,
                routing NVARCHAR(MAX) NULL,
                quantity INT NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_job_labels_job_folders FOREIGN KEY (job_folder_id) REFERENCES dbo.job_folders(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.job_orders', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.job_orders (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                job_folder_id INT NOT NULL,
                source_type NVARCHAR(255) NOT NULL,
                part_number NVARCHAR(255) NULL,
                material NVARCHAR(255) NULL,
                profile NVARCHAR(255) NULL,
                order_group NVARCHAR(255) NULL,
                raw_identifier NVARCHAR(255) NULL,
                quantity INT NULL,
                length FLOAT NULL,
                source_file_path NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_job_orders_job_folders FOREIGN KEY (job_folder_id) REFERENCES dbo.job_folders(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.resolved_nest_parts', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.resolved_nest_parts (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                nest_id INT NOT NULL,
                nest_part_id INT NOT NULL UNIQUE,
                barcode_filename NVARCHAR(255) NOT NULL,
                build_date_code NVARCHAR(255) NULL,
                order_number_raw NVARCHAR(255) NULL,
                part_number NVARCHAR(255) NOT NULL,
                part_revision NVARCHAR(255) NULL,
                quantity_nested INT NOT NULL DEFAULT 0,
                com_number NVARCHAR(255) NULL,
                form_value NVARCHAR(255) NULL,
                requires_forming INT NULL,
                nested_on NVARCHAR(255) NULL,
                resolution_status NVARCHAR(255) NOT NULL,
                resolution_rule NVARCHAR(255) NULL,
                match_candidate_count INT NOT NULL DEFAULT 0,
                match_build_dates NVARCHAR(MAX) NULL,
                match_com_numbers NVARCHAR(MAX) NULL,
                matched_job_folder_id INT NULL,
                matched_job_part_id INT NULL,
                job_match_score INT NOT NULL DEFAULT 0,
                evidence_summary NVARCHAR(MAX) NULL,
                matched_part_attribute_id INT NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_resolved_nest_parts_program_nests FOREIGN KEY (nest_id) REFERENCES dbo.program_nests(id) ON DELETE CASCADE,
                CONSTRAINT FK_resolved_nest_parts_nest_parts FOREIGN KEY (nest_part_id) REFERENCES dbo.nest_parts(id) ON DELETE CASCADE,
                CONSTRAINT FK_resolved_nest_parts_job_folders FOREIGN KEY (matched_job_folder_id) REFERENCES dbo.job_folders(id) ON DELETE SET NULL,
                CONSTRAINT FK_resolved_nest_parts_job_parts FOREIGN KEY (matched_job_part_id) REFERENCES dbo.job_parts(id) ON DELETE SET NULL,
                CONSTRAINT FK_resolved_nest_parts_part_attributes FOREIGN KEY (matched_part_attribute_id) REFERENCES dbo.part_attributes(id) ON DELETE SET NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.nest_part_enrichment', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.nest_part_enrichment (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                nest_part_id INT NOT NULL,
                part_attribute_id INT NOT NULL,
                match_method NVARCHAR(255) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT UQ_nest_part_enrichment UNIQUE (nest_part_id, part_attribute_id),
                CONSTRAINT FK_nest_part_enrichment_nest_parts FOREIGN KEY (nest_part_id) REFERENCES dbo.nest_parts(id) ON DELETE CASCADE,
                CONSTRAINT FK_nest_part_enrichment_part_attributes FOREIGN KEY (part_attribute_id) REFERENCES dbo.part_attributes(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.flat_scan_sessions', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.flat_scan_sessions (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                nest_id INT NOT NULL,
                run_number INT NOT NULL DEFAULT 1,
                started_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at DATETIME2(0) NULL,
                status NVARCHAR(50) NOT NULL DEFAULT 'open',
                CONSTRAINT FK_flat_scan_sessions_program_nests FOREIGN KEY (nest_id) REFERENCES dbo.program_nests(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.flat_scan_items', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.flat_scan_items (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                flat_scan_session_id INT NOT NULL,
                nest_part_id INT NOT NULL,
                expected_quantity INT NOT NULL,
                scanned_quantity INT NOT NULL DEFAULT 0,
                is_complete INT NOT NULL DEFAULT 0,
                requires_forming INT NOT NULL DEFAULT 0,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT UQ_flat_scan_items_session_part UNIQUE (flat_scan_session_id, nest_part_id),
                CONSTRAINT FK_flat_scan_items_sessions FOREIGN KEY (flat_scan_session_id) REFERENCES dbo.flat_scan_sessions(id) ON DELETE CASCADE,
                CONSTRAINT FK_flat_scan_items_nest_parts FOREIGN KEY (nest_part_id) REFERENCES dbo.nest_parts(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.forming_batches', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.forming_batches (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                batch_code NVARCHAR(255) NOT NULL UNIQUE,
                source_nest_id INT NULL,
                com_number NVARCHAR(255) NULL,
                build_date_code NVARCHAR(255) NULL,
                status NVARCHAR(50) NOT NULL DEFAULT 'queued',
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME2(0) NULL,
                completed_at DATETIME2(0) NULL,
                CONSTRAINT FK_forming_batches_program_nests FOREIGN KEY (source_nest_id) REFERENCES dbo.program_nests(id) ON DELETE SET NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.forming_batch_items', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.forming_batch_items (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                forming_batch_id INT NOT NULL,
                nest_part_id INT NULL,
                part_attribute_id INT NULL,
                part_number NVARCHAR(255) NOT NULL,
                part_revision NVARCHAR(255) NULL,
                expected_quantity INT NOT NULL DEFAULT 0,
                scanned_quantity INT NOT NULL DEFAULT 0,
                is_complete INT NOT NULL DEFAULT 0,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_forming_batch_items_batches FOREIGN KEY (forming_batch_id) REFERENCES dbo.forming_batches(id) ON DELETE CASCADE,
                CONSTRAINT FK_forming_batch_items_nest_parts FOREIGN KEY (nest_part_id) REFERENCES dbo.nest_parts(id) ON DELETE SET NULL,
                CONSTRAINT FK_forming_batch_items_part_attributes FOREIGN KEY (part_attribute_id) REFERENCES dbo.part_attributes(id) ON DELETE SET NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.monitor_units', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.monitor_units (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                com_number NVARCHAR(255) NOT NULL UNIQUE,
                status NVARCHAR(50) NOT NULL DEFAULT 'in_progress',
                started_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_activity_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.monitor_unit_sources', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.monitor_unit_sources (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                monitor_unit_id INT NOT NULL,
                barcode_filename NVARCHAR(255) NOT NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT UQ_monitor_unit_sources UNIQUE (monitor_unit_id, barcode_filename),
                CONSTRAINT FK_monitor_unit_sources_monitor_units FOREIGN KEY (monitor_unit_id) REFERENCES dbo.monitor_units(id) ON DELETE CASCADE
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.scan_events', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.scan_events (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                event_type NVARCHAR(255) NOT NULL,
                barcode_value NVARCHAR(255) NOT NULL,
                part_number NVARCHAR(255) NULL,
                part_revision NVARCHAR(255) NULL,
                flat_scan_session_id INT NULL,
                forming_batch_id INT NULL,
                notes NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_scan_events_sessions FOREIGN KEY (flat_scan_session_id) REFERENCES dbo.flat_scan_sessions(id) ON DELETE SET NULL,
                CONSTRAINT FK_scan_events_forming_batches FOREIGN KEY (forming_batch_id) REFERENCES dbo.forming_batches(id) ON DELETE SET NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.part_tracker_items', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.part_tracker_items (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                tracker_key NVARCHAR(450) NOT NULL UNIQUE,
                flat_scan_session_id INT NULL,
                run_number INT NOT NULL DEFAULT 1,
                dat_name NVARCHAR(255) NOT NULL,
                nest_part_id INT NULL,
                scan_sequence INT NOT NULL DEFAULT 1,
                part_number NVARCHAR(255) NOT NULL,
                part_revision NVARCHAR(255) NULL,
                com_number NVARCHAR(255) NULL,
                machine NVARCHAR(255) NULL,
                user_code NVARCHAR(255) NULL,
                location NVARCHAR(255) NULL,
                requires_forming INT NOT NULL DEFAULT 0,
                stage NVARCHAR(50) NOT NULL DEFAULT 'Prog',
                stage_updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT FK_part_tracker_items_sessions FOREIGN KEY (flat_scan_session_id) REFERENCES dbo.flat_scan_sessions(id) ON DELETE SET NULL,
                CONSTRAINT FK_part_tracker_items_nest_parts FOREIGN KEY (nest_part_id) REFERENCES dbo.nest_parts(id) ON DELETE SET NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.part_tracker_history', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.part_tracker_history (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                tracker_key NVARCHAR(450) NOT NULL,
                history_group_key NVARCHAR(450) NOT NULL,
                event_type NVARCHAR(255) NOT NULL,
                scanner_name NVARCHAR(255) NOT NULL DEFAULT 'system',
                dat_name NVARCHAR(255) NOT NULL,
                run_number INT NOT NULL DEFAULT 1,
                nest_part_id INT NULL,
                scan_sequence INT NOT NULL DEFAULT 1,
                part_number NVARCHAR(255) NOT NULL,
                part_revision NVARCHAR(255) NULL,
                com_number NVARCHAR(255) NULL,
                machine NVARCHAR(255) NULL,
                user_code NVARCHAR(255) NULL,
                location NVARCHAR(255) NULL,
                requires_forming INT NOT NULL DEFAULT 0,
                stage NVARCHAR(50) NOT NULL DEFAULT 'Prog',
                recorded_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                notes NVARCHAR(MAX) NULL
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.processed_files', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.processed_files (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                file_path NVARCHAR(450) NOT NULL UNIQUE,
                file_name NVARCHAR(255) NOT NULL,
                file_type NVARCHAR(255) NOT NULL,
                file_size BIGINT NOT NULL,
                modified_time FLOAT NOT NULL,
                content_hash NVARCHAR(255) NULL,
                status NVARCHAR(50) NOT NULL,
                last_error NVARCHAR(MAX) NULL,
                processed_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.import_runs', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.import_runs (
                id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                trigger NVARCHAR(50) NOT NULL,
                status NVARCHAR(50) NOT NULL,
                message NVARCHAR(MAX) NOT NULL,
                started_at DATETIME2(0) NOT NULL,
                completed_at DATETIME2(0) NULL,
                active_paths_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                missing_paths_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                processed INT NOT NULL DEFAULT 0,
                skipped INT NOT NULL DEFAULT 0,
                errors INT NOT NULL DEFAULT 0,
                missing_files INT NOT NULL DEFAULT 0,
                total_supported_files INT NOT NULL DEFAULT 0,
                nest_files INT NOT NULL DEFAULT 0,
                dat_files INT NOT NULL DEFAULT 0,
                dat_groups INT NOT NULL DEFAULT 0,
                duplicate_dat_files INT NOT NULL DEFAULT 0,
                filtered_old_files INT NOT NULL DEFAULT 0,
                unstable_recent_files INT NOT NULL DEFAULT 0,
                last_error NVARCHAR(MAX) NULL,
                created_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF OBJECT_ID(N'dbo.ui_session_state', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.ui_session_state (
                session_key NVARCHAR(255) NOT NULL PRIMARY KEY,
                state_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
                completed_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                missed_json NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                updated_at DATETIME2(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_nest_parts_part_number' AND object_id = OBJECT_ID(N'dbo.nest_parts'))
        BEGIN
            CREATE INDEX idx_nest_parts_part_number ON dbo.nest_parts(part_number, part_revision)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_attributes_lookup' AND object_id = OBJECT_ID(N'dbo.part_attributes'))
        BEGIN
            CREATE INDEX idx_part_attributes_lookup ON dbo.part_attributes(part_number, rev_level, build_date, com_number)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_folders_lookup' AND object_id = OBJECT_ID(N'dbo.job_folders'))
        BEGIN
            CREATE INDEX idx_job_folders_lookup ON dbo.job_folders(com_number, build_date_code)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_parts_lookup' AND object_id = OBJECT_ID(N'dbo.job_parts'))
        BEGIN
            CREATE INDEX idx_job_parts_lookup ON dbo.job_parts(part_number, revision_key, build_date_code, order_number_raw)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_labels_lookup' AND object_id = OBJECT_ID(N'dbo.job_labels'))
        BEGIN
            CREATE INDEX idx_job_labels_lookup ON dbo.job_labels(part_number, unit_id, build_day)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_orders_lookup' AND object_id = OBJECT_ID(N'dbo.job_orders'))
        BEGIN
            CREATE INDEX idx_job_orders_lookup ON dbo.job_orders(part_number, source_type, order_group)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_forming_batch_items_lookup' AND object_id = OBJECT_ID(N'dbo.forming_batch_items'))
        BEGIN
            CREATE INDEX idx_forming_batch_items_lookup ON dbo.forming_batch_items(part_number, part_revision)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_forming_batch_items_batch_nest' AND object_id = OBJECT_ID(N'dbo.forming_batch_items'))
        BEGIN
            CREATE UNIQUE INDEX idx_forming_batch_items_batch_nest ON dbo.forming_batch_items(forming_batch_id, nest_part_id)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_monitor_units_status_activity' AND object_id = OBJECT_ID(N'dbo.monitor_units'))
        BEGIN
            CREATE INDEX idx_monitor_units_status_activity ON dbo.monitor_units(status, last_activity_at DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_monitor_sources_barcode' AND object_id = OBJECT_ID(N'dbo.monitor_unit_sources'))
        BEGIN
            CREATE INDEX idx_monitor_sources_barcode ON dbo.monitor_unit_sources(barcode_filename)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_tracker_search' AND object_id = OBJECT_ID(N'dbo.part_tracker_items'))
        BEGIN
            CREATE INDEX idx_part_tracker_search ON dbo.part_tracker_items(part_number, com_number, stage, dat_name)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_tracker_updated' AND object_id = OBJECT_ID(N'dbo.part_tracker_items'))
        BEGIN
            CREATE INDEX idx_part_tracker_updated ON dbo.part_tracker_items(stage_updated_at DESC, updated_at DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_tracker_history_tracker' AND object_id = OBJECT_ID(N'dbo.part_tracker_history'))
        BEGIN
            CREATE INDEX idx_part_tracker_history_tracker ON dbo.part_tracker_history(tracker_key, recorded_at DESC, id DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_tracker_history_group' AND object_id = OBJECT_ID(N'dbo.part_tracker_history'))
        BEGIN
            CREATE INDEX idx_part_tracker_history_group ON dbo.part_tracker_history(history_group_key, recorded_at DESC, id DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_processed_files_status' AND object_id = OBJECT_ID(N'dbo.processed_files'))
        BEGIN
            CREATE INDEX idx_processed_files_status ON dbo.processed_files(status, file_type)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_import_runs_started_at' AND object_id = OBJECT_ID(N'dbo.import_runs'))
        BEGIN
            CREATE INDEX idx_import_runs_started_at ON dbo.import_runs(started_at DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_import_runs_status_started_at' AND object_id = OBJECT_ID(N'dbo.import_runs'))
        BEGIN
            CREATE INDEX idx_import_runs_status_started_at ON dbo.import_runs(status, started_at DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_ui_session_state_updated' AND object_id = OBJECT_ID(N'dbo.ui_session_state'))
        BEGIN
            CREATE INDEX idx_ui_session_state_updated ON dbo.ui_session_state(updated_at DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_attributes_resolve_lookup' AND object_id = OBJECT_ID(N'dbo.part_attributes'))
        BEGIN
            CREATE INDEX idx_part_attributes_resolve_lookup ON dbo.part_attributes(part_number, normalized_rev_key, build_date)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_attributes_part_com_lookup' AND object_id = OBJECT_ID(N'dbo.part_attributes'))
        BEGIN
            CREATE INDEX idx_part_attributes_part_com_lookup ON dbo.part_attributes(part_number, com_number)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_parts_resolve_lookup' AND object_id = OBJECT_ID(N'dbo.job_parts'))
        BEGIN
            CREATE INDEX idx_job_parts_resolve_lookup ON dbo.job_parts(part_number, revision_key, job_folder_id)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_parts_job_folder_part_lookup' AND object_id = OBJECT_ID(N'dbo.job_parts'))
        BEGIN
            CREATE INDEX idx_job_parts_job_folder_part_lookup ON dbo.job_parts(job_folder_id, part_number)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_job_labels_job_folder_part_lookup' AND object_id = OBJECT_ID(N'dbo.job_labels'))
        BEGIN
            CREATE INDEX idx_job_labels_job_folder_part_lookup ON dbo.job_labels(job_folder_id, part_number)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_resolved_nest_parts_barcode' AND object_id = OBJECT_ID(N'dbo.resolved_nest_parts'))
        BEGIN
            CREATE INDEX idx_resolved_nest_parts_barcode ON dbo.resolved_nest_parts(barcode_filename, part_number)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_resolved_nest_parts_com_number' AND object_id = OBJECT_ID(N'dbo.resolved_nest_parts'))
        BEGIN
            CREATE INDEX idx_resolved_nest_parts_com_number ON dbo.resolved_nest_parts(com_number, barcode_filename)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_flat_scan_sessions_nest_run' AND object_id = OBJECT_ID(N'dbo.flat_scan_sessions'))
        BEGIN
            CREATE INDEX idx_flat_scan_sessions_nest_run ON dbo.flat_scan_sessions(nest_id, run_number DESC, id DESC)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_flat_scan_items_nest_part' AND object_id = OBJECT_ID(N'dbo.flat_scan_items'))
        BEGIN
            CREATE INDEX idx_flat_scan_items_nest_part ON dbo.flat_scan_items(nest_part_id, scanned_quantity)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_forming_batch_items_nest_part' AND object_id = OBJECT_ID(N'dbo.forming_batch_items'))
        BEGIN
            CREATE INDEX idx_forming_batch_items_nest_part ON dbo.forming_batch_items(nest_part_id, scanned_quantity)
        END
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_part_tracker_dat_sequence' AND object_id = OBJECT_ID(N'dbo.part_tracker_items'))
        BEGIN
            CREATE INDEX idx_part_tracker_dat_sequence ON dbo.part_tracker_items(dat_name, run_number, nest_part_id, scan_sequence)
        END
        """,
    ]
    _execute_sqlserver_statements(connection, create_statements)


def create_schema(connection: Any) -> None:
    if _schema_backend() == "sqlserver":
        _create_schema_sqlserver(connection)
        return

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
            run_number INTEGER NOT NULL DEFAULT 1,
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

        CREATE TABLE IF NOT EXISTS monitor_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            com_number TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'in_progress',
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_activity_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS monitor_unit_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            monitor_unit_id INTEGER NOT NULL,
            barcode_filename TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (monitor_unit_id) REFERENCES monitor_units(id) ON DELETE CASCADE,
            UNIQUE (monitor_unit_id, barcode_filename)
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

        CREATE TABLE IF NOT EXISTS part_tracker_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker_key TEXT NOT NULL UNIQUE,
            flat_scan_session_id INTEGER,
            run_number INTEGER NOT NULL DEFAULT 1,
            dat_name TEXT NOT NULL,
            nest_part_id INTEGER,
            scan_sequence INTEGER NOT NULL DEFAULT 1,
            part_number TEXT NOT NULL,
            part_revision TEXT,
            com_number TEXT,
            machine TEXT,
            user_code TEXT,
            location TEXT,
            requires_forming INTEGER NOT NULL DEFAULT 0,
            stage TEXT NOT NULL DEFAULT 'Prog',
            stage_updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (flat_scan_session_id) REFERENCES flat_scan_sessions(id) ON DELETE SET NULL,
            FOREIGN KEY (nest_part_id) REFERENCES nest_parts(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS part_tracker_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracker_key TEXT NOT NULL,
            history_group_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            scanner_name TEXT NOT NULL DEFAULT 'system',
            dat_name TEXT NOT NULL,
            run_number INTEGER NOT NULL DEFAULT 1,
            nest_part_id INTEGER,
            scan_sequence INTEGER NOT NULL DEFAULT 1,
            part_number TEXT NOT NULL,
            part_revision TEXT,
            com_number TEXT,
            machine TEXT,
            user_code TEXT,
            location TEXT,
            requires_forming INTEGER NOT NULL DEFAULT 0,
            stage TEXT NOT NULL DEFAULT 'Prog',
            recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
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

        CREATE TABLE IF NOT EXISTS ui_session_state (
            session_key TEXT PRIMARY KEY,
            state_json TEXT NOT NULL DEFAULT '{}',
            completed_json TEXT NOT NULL DEFAULT '[]',
            missed_json TEXT NOT NULL DEFAULT '[]',
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

        CREATE UNIQUE INDEX IF NOT EXISTS idx_forming_batch_items_batch_nest
            ON forming_batch_items(forming_batch_id, nest_part_id);

        CREATE INDEX IF NOT EXISTS idx_monitor_units_status_activity
            ON monitor_units(status, last_activity_at DESC);

        CREATE INDEX IF NOT EXISTS idx_monitor_sources_barcode
            ON monitor_unit_sources(barcode_filename);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_search
            ON part_tracker_items(part_number, com_number, stage, dat_name);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_updated
            ON part_tracker_items(stage_updated_at DESC, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_history_tracker
            ON part_tracker_history(tracker_key, recorded_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_history_group
            ON part_tracker_history(history_group_key, recorded_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_processed_files_status
            ON processed_files(status, file_type);

        CREATE INDEX IF NOT EXISTS idx_import_runs_started_at
            ON import_runs(started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_import_runs_status_started_at
            ON import_runs(status, started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_ui_session_state_updated
            ON ui_session_state(updated_at DESC);
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

    existing_flat_scan_session_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(flat_scan_sessions)").fetchall()
    }
    if "run_number" not in existing_flat_scan_session_columns:
        connection.execute(
            "ALTER TABLE flat_scan_sessions ADD COLUMN run_number INTEGER NOT NULL DEFAULT 1"
        )

    existing_part_tracker_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(part_tracker_items)").fetchall()
    }
    if "run_number" not in existing_part_tracker_columns:
        connection.execute(
            "ALTER TABLE part_tracker_items ADD COLUMN run_number INTEGER NOT NULL DEFAULT 1"
        )

    connection.execute(
        """
        DELETE FROM part_tracker_items
        WHERE tracker_key NOT LIKE 'legacy|%'
          AND tracker_key NOT LIKE '%|run%|%'
          AND EXISTS (
              SELECT 1
              FROM part_tracker_items existing
              WHERE existing.tracker_key =
                    UPPER(TRIM(COALESCE(part_tracker_items.dat_name, '')))
                    || '|run' || CAST(COALESCE(part_tracker_items.run_number, 1) AS TEXT)
                    || '|'
                    || CASE
                        WHEN part_tracker_items.nest_part_id IS NULL THEN 'legacy'
                        ELSE CAST(part_tracker_items.nest_part_id AS TEXT)
                       END
                    || '|'
                    || CAST(COALESCE(part_tracker_items.scan_sequence, 1) AS TEXT)
          )
        """
    )
    connection.execute(
        """
        UPDATE OR IGNORE part_tracker_items
        SET tracker_key =
            UPPER(TRIM(COALESCE(dat_name, '')))
            || '|run' || CAST(COALESCE(run_number, 1) AS TEXT)
            || '|'
            || CASE
                WHEN nest_part_id IS NULL THEN 'legacy'
                ELSE CAST(nest_part_id AS TEXT)
               END
            || '|'
            || CAST(COALESCE(scan_sequence, 1) AS TEXT)
        WHERE tracker_key NOT LIKE 'legacy|%'
          AND tracker_key NOT LIKE '%|run%|%'
        """
    )
    connection.execute(
        """
        INSERT INTO part_tracker_history (
            tracker_key,
            history_group_key,
            event_type,
            scanner_name,
            dat_name,
            run_number,
            nest_part_id,
            scan_sequence,
            part_number,
            part_revision,
            com_number,
            machine,
            user_code,
            location,
            requires_forming,
            stage,
            recorded_at,
            notes
        )
        SELECT
            part_tracker_items.tracker_key,
            UPPER(TRIM(COALESCE(part_tracker_items.dat_name, '')))
                || '|'
                || CASE
                    WHEN part_tracker_items.nest_part_id IS NULL THEN 'legacy'
                    ELSE CAST(part_tracker_items.nest_part_id AS TEXT)
                   END
                || '|'
                || CAST(COALESCE(part_tracker_items.scan_sequence, 1) AS TEXT),
            'baseline',
            'system',
            COALESCE(part_tracker_items.dat_name, ''),
            COALESCE(part_tracker_items.run_number, 1),
            part_tracker_items.nest_part_id,
            COALESCE(part_tracker_items.scan_sequence, 1),
            COALESCE(part_tracker_items.part_number, ''),
            COALESCE(part_tracker_items.part_revision, '-'),
            COALESCE(part_tracker_items.com_number, ''),
            COALESCE(part_tracker_items.machine, ''),
            COALESCE(part_tracker_items.user_code, ''),
            COALESCE(part_tracker_items.location, ''),
            COALESCE(part_tracker_items.requires_forming, 0),
            COALESCE(part_tracker_items.stage, 'Prog'),
            COALESCE(part_tracker_items.stage_updated_at, part_tracker_items.updated_at, part_tracker_items.created_at, CURRENT_TIMESTAMP),
            'Baseline history entry'
        FROM part_tracker_items
        WHERE NOT EXISTS (
            SELECT 1
            FROM part_tracker_history existing
            WHERE existing.tracker_key = part_tracker_items.tracker_key
        )
        """
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

        CREATE INDEX IF NOT EXISTS idx_resolved_nest_parts_com_number
            ON resolved_nest_parts(com_number, barcode_filename);

        CREATE INDEX IF NOT EXISTS idx_flat_scan_sessions_nest_run
            ON flat_scan_sessions(nest_id, run_number DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_flat_scan_items_nest_part
            ON flat_scan_items(nest_part_id, scanned_quantity);

        CREATE INDEX IF NOT EXISTS idx_forming_batch_items_nest_part
            ON forming_batch_items(nest_part_id, scanned_quantity);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_dat_sequence
            ON part_tracker_items(dat_name, run_number, nest_part_id, scan_sequence);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_history_tracker
            ON part_tracker_history(tracker_key, recorded_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_part_tracker_history_group
            ON part_tracker_history(history_group_key, recorded_at DESC, id DESC);
        """
    )
