# NEWTRACKER Application

This folder contains the local application bootstrap for the NEWTRACKER database.

## What it does

- Creates a SQLite database for pre-extracted production data
- Stores nest/sheet records, part records, manufacturing attributes, and scan tracking tables
- Prepares the project for later file-watching and import/parsing work

## Current status

This workspace now supports:

- importing `.DAT` and `NestComparison.csv` data into SQLite
- rebuilding SQL-only nest/part lookup data
- generating printable test barcodes for scan values like `.DAT` filenames
- running a minimal clean Flask UI for user/location/nest/part scanning

## Database location

The SQLite file is created at:

- `Application/data/newtracker.db`

## Run

Use the workspace virtual environment Python interpreter to run:

- `Application/scripts/init_db.py`
- `Application/scripts/import_test_data.py`

## Run the clean scan UI

Run:

- `Application/scripts/run_clean_ui.py`

Then open:

- `http://127.0.0.1:5000/`

Current flow:

- scan `USER`
- scan `LOCATION`
- scan `NEST DATA`
- expected parts load underneath from SQL `resolved_nest_parts`
- scan part barcodes into the `PART SCAN` field
- expected parts stay on the left with reduced remaining counts
- scanned parts accumulate on the right

Scanner UI state is now stored per browser session under:

- `Application/data/ui_sessions/<session-key>/ui_scan_state.json`

Completed scan archives are stored beside the session state file:

- `Application/data/ui_sessions/<session-key>/completed_scan_list.json`

Admin security settings are stored in:

- `Application/data/admin_settings.json`

Existing installs keep the legacy bootstrap admin login until it is changed from the Admin Settings page.

## Generate test barcode sheets

To generate printable Code 128 barcode sheets for:

- test users
- machines
- locations
- DAT scan pages with one DAT barcode on top and formed / not formed sections on the same page

run:

- `Application/scripts/generate_barcodes.py`

By default it writes PDF sheet files into:

- `Application/data/barcode_sheets/`

Behavior:

- generates master barcode sheets for `STH`, `JAL`
- generates master barcode sheets for machines and locations
- generates one DAT barcode at the top of each DAT page
- repeats part barcode labels by `quantity_nested`
- places `NOT FORMED` above a divider and `FORMED` below it on the same page using SQL-only data

You can also pass specific DAT filenames to generate just those sheets.

The current layout uses wider, longer Code 128 barcodes to improve printed scanner readability.

## Notes

- The expected parts still come from your SQL-only import pipeline.

## Changelog

Project changes are tracked in:

- `CHANGELOG.md`

The admin UI also exposes the same changelog from the Admin Settings page.

## Import rules reference

For detailed, source-of-truth behavior of import/canonicalization logic (including duplicate DAT filename handling), see:

- `Application/IMPORT_RULES.md`
