# Changelog

This file tracks notable changes made to the NEWTRACKER application during active development.

## 2026-04-21

### Fixed

- Fixed auto-import failures on Windows where transient `Access is denied` errors during `import_scan_cache.json` replacement could abort the whole import while scanning folders.
- Atomic sidecar-file writes now retry transient replace failures briefly, which helps when antivirus, indexing, or another process momentarily holds the target file open.
- Import scan-cache writes are now best-effort only, so a locked cache sidecar no longer prevents DAT and job-file imports from continuing.
- Import scan-cache write warnings and import failures now write diagnostic report files to the configured admin report folder even when the warning is non-fatal.
- The admin `Clear All Parsed Data` action now asks for confirmation before wiping parsed database content.

### Added

- Added an admin `Send Test Log` action so the configured report folder can be verified on local or network paths without waiting for a real failure.

### Changed

- Main-scanner progress is now treated as resumable only after at least one part has actually been scanned; loading a DAT with zero scanned parts no longer creates durable in-progress progress state.
- Fresh browser sessions now auto-resume the latest scanned main-scanner batch, and the home page can offer a resume card when a blank session can recover saved scanned work.

### Validation

- Verified the updated Python package compiles locally.
- Verified the atomic write helper retries a transient replace failure and still completes the write.
- Verified `save_scan_cache()` no longer raises when the cache write path throws `PermissionError`.
- Verified a manual test log can be created through the configured report-writer path.
- Verified a fresh session no longer auto-resumes zero-scan DAT loads, while a saved scanned batch can still be auto-resumed.
- Verified the completed-list tracker cleanup removed the stale zero-scan `Prog` rows and left zero remaining `Prog` groups in the live database.
- Verified lowercase-vs-uppercase part scans now match the same expected part number in both the main scanner and formed scanner flows.

### Fixed

- Fixed main-scanner DAT switching so once any part from the current batch has been scanned, scanning a different DAT is blocked until the current batch is completed or force-completed.
- Fixed abandoned zero-scan DAT loads leaving stale `Prog` tracker rows in the completed list by discarding those transient flat-scan sessions and their tracker history.
- Fixed startup/session cleanup so stale zero-scan saved scanner state is cleared automatically instead of repeatedly resurfacing abandoned DAT loads.
- Fixed case-sensitive part matching in the main scanner and formed scanner so scanned barcodes with lowercase letters still match the uppercase part numbers loaded from DAT data.

## 2026-04-20

### Changed

- The formed scanner now uses a single scan input that accepts either a DAT token or a formed part barcode instead of separate scan boxes.
- Formed scanner routing now detects known DAT scans server-side and otherwise treats the value as a part scan, so DAT loads, unique part matches, and ambiguous part selection all stay in one flow.

### Added

- Added a persisted `scanner_auto_mode` admin setting with `Off`, `Auto Complete`, and `Full Auto` modes for the main scanner.
- Added an admin three-way selector for main-scanner automation so operators can decide whether DAT loads stay manual, auto-move expected parts into Scanned, or auto-complete the batch to `Cut`.
- Added main-scanner auto-fill behavior that can move every expected part into the scanned list immediately after a DAT load or repeat-run confirmation.
- Added admin-controlled debug diagnostics with an on/off capture toggle, a configurable error-report folder, automatic traceback report files, and admin download links for recent reports.

### Fixed

- Fixed admin import-status polling so it no longer hits the database schema path during active imports, preventing transient `database is locked` failures while the import is still running.
- Fixed first-run scanner initialization so empty legacy tracker migration no longer touches SQLite unnecessarily, and busy tracker migration attempts now defer instead of crashing request startup.

### Validation

- Verified the edited Python, template, and CSS files reported no workspace errors.
- Verified the Flask app restarted and responded on `http://127.0.0.1:5000` after the changes.
- Ran an isolated temporary-file validation covering `AdminSettingsStore.update_from_form()` persistence and `UiStateStore.auto_fill_current_batch()` behavior without mutating live tracker data.
- Verified the admin page renders the new debug-report controls and report list sections.

## 2026-04-17

### Changed

- The monitor dashboard's CSV estimate now uses raw imported `nest_comparison` rows from `job_parts` instead of the collapsed `part_attributes.quantity_per` value.
- The CSV estimate now follows the same source ignore rules used by imports, so ignored paths and nested `OLD` folders no longer inflate monitor totals.
- The formed scanner queue now renders as a condensed COM-first card grid that stays usable when many DAT lists are waiting.
- Formed scanner success feedback now stays on the page message card so DAT-driven loads, automatic part-driven loads, and ambiguity handling all report the correct state.

### Added

- Added direct part-first list selection on the formed scanner so scanning a part can automatically load the matching DAT list when there is a unique match.
- Added a red conflict state on the formed scanner for ambiguous part scans, with large clickable and scannable DAT choices that can resolve the correct list.
- Added a filtered `Estimated CSV Nested Parts` monitor metric that excludes `Not Nested` rows and `Eclipse` rows on `Walls_Channels`.

### Fixed

- Fixed monitor CSV estimate undercounting caused by repeated raw CSV rows collapsing to `1` through the `part_attributes` upsert key.
- Fixed monitor CSV estimate overcounting caused by stale `OLD` programming-folder copies being included in the display-only estimate.

### Validation

- Verified the updated package with `python -m compileall Application/src/newtracker`.
- Smoke-tested formed scanner state loading with `UiStateStore(session_key='formed-ui-smoke').formed_context()`.
- Verified the Flask app factory still initializes via `from newtracker.ui_app import create_ui_app; create_ui_app()`.

## 2026-04-16

### Changed

- Reworked the scanner workflow so DAT scans create shared SQLite-backed part tracker rows instead of relying on the old archived completed-list JSON flow.
- The completed list now acts as a searchable tracker view with stage-aware rows, run numbers, latest-vs-older run coloring, and direct part-history links.
- The main scanner now supports session-only scanned-part editing before submit, plus explicit Complete and Force Complete actions.
- The formed scanner now loads from the latest tracker-backed run for formed parts instead of rebuilding its active list directly from resolved DAT rows.

### Added

- Added `run_number` support for repeated DAT scans so the same program can be run again without overwriting prior tracker history.
- Added duplicate DAT confirmation on the main scanner when a finalized run already exists for that DAT.
- Added `part_tracker_history` with baseline and stage-change snapshots so each tracked part can be audited over time.
- Added a new completed-list history page for drilling into one part's recorded stage and edit history.
- Added a `Formed` tracker stage so formed completion is represented directly in the shared tracker.

### Fixed

- Fixed the runtime schema migration order so `run_number` columns are added before indexes that depend on them, preventing SQLite startup failures on older databases.
- Fixed formed-scanner routes that previously pointed at missing review/store methods by wiring them to the new per-batch edit, complete, and force-complete flow.
- Fixed tracker updates to preserve the edited part, COM, machine, user, and location values that belong to the scanned row being submitted.

### Validation

- Verified the updated package with `python -m compileall Application/src/newtracker`.
- Smoke-tested the live Flask app for the completed tracker, part-history page, formed scanner page, and tracker-backed formed DAT load path.

## 2026-04-14

### Recorded From Git

- Backfilled from git commit `a117cf4` on `main` / `origin/main`.
- Commit subject: `Snapshot current importer and UI state`.

### Changed

- Updated the importer, parser, schema, admin settings flow, and UI state handling as part of a broad application snapshot.
- Updated the clean Flask UI entrypoint and core UI routes.
- Updated the admin page, completed list, formed scanner, and main index templates.
- Updated the database/bootstrap scripts and application README to match that snapshot state.

### Files Touched In Snapshot

- `Application/scripts/init_db.py`
- `Application/scripts/run_clean_ui.py`
- `Application/src/newtracker/admin_settings.py`
- `Application/src/newtracker/importer.py`
- `Application/src/newtracker/parser.py`
- `Application/src/newtracker/schema.py`
- `Application/src/newtracker/ui_app.py`
- `Application/src/newtracker/ui_state.py`
- `Application/src/newtracker/ui/templates/admin.html`
- `Application/src/newtracker/ui/templates/completed_list.html`
- `Application/src/newtracker/ui/templates/formed_scanner.html`
- `Application/src/newtracker/ui/templates/index.html`

## 2026-04-15

### Fixed

- Reduced resolved part rebuild time by preloading job folders, job parts, job labels, and part attributes into in-memory lookup maps during resolution.
- Reduced no-change import runtime on network shares by switching folder scanning to `os.scandir()` and `DirEntry` metadata instead of repeated `Path.iterdir()` and `Path.stat()` calls.
- Restored discovery of newly published immutable Laser DAT files when the network share root mtime did not update reliably.
- Changed no-change manual import messaging from `Imported 0 files.` to `No new files found.`
- Added a deferred-file retry path for very recent files so files modified within the stability window are retried on later scans.
- Added hard-fail behavior when the immutable `P:\Manufacturing\CNC` source tree becomes unavailable during scan or import, preventing partial successful imports after source access loss.
- Added startup aborts when required P-drive source roots are unavailable before an import begins.
- Added recovery for interrupted imports by marking stale `running` import rows as `interrupted` on app startup.
- Moved the admin credential update form off the main admin page onto its own admin security screen.
- Restored the completed-list page by adding the missing missed-scan list storage methods used by the route and clear action.

### Changed

- Normal imports now treat production network files as append-only immutable inputs: already processed immutable files are skipped and already imported DAT groups are not re-imported.
- Normal DAT resolution now processes only newly imported nests instead of rebuilding `resolved_nest_parts` globally after every run.
- Scanner UI state is now stored per browser session under `Application/data/ui_sessions/<session-key>/` instead of one shared global JSON file.
- Admin login now uses persisted security settings with a stored password hash and persisted Flask secret key.
- The development launcher keeps Flask debug enabled by default; the earlier debug-off change was reverted so existing dev behavior is preserved.
- The admin changelog screen now renders structured release-note cards instead of a raw markdown text block.
- The admin changelog screen now includes a print action with print-specific layout rules for paper or Save as PDF output.
- DAT scans now create or reuse persisted flat-scan and forming progress records in SQLite, so unit progress is no longer tracked only in session JSON.
- The COM monitor dashboard now reads started units and live progress from SQLite, making it shared across sessions instead of tied to one browser session.
- Main scanner controls now distinguish between session reset, completed-list cleanup, and full development progress cleanup so in-progress SQL monitor state is not mistaken for the archived list.

### Added

- Added atomic JSON/text persistence helpers so settings, scanner state, and import cache files use atomic replace writes.
- Added corrupt-file fallback behavior for JSON sidecar files so invalid state files do not crash the app on read.
- Added an `import_runs` SQLite table to track import lifecycle state, result summaries, and interrupted runs.
- Added a dedicated admin security page for changing the local admin username and password.
- Added a dedicated admin changelog page and admin-page links to this file.
- Added a COM monitor dashboard page that starts tracking whole units when DAT files are scanned and shows per-unit part/forming progress.
- Added a main-scanner `Clear Dev Progress` action that wipes SQL-backed scan/monitor progress while keeping imported part definitions intact.
- Added this `CHANGELOG.md` file and backfilled the recent import, reliability, and admin UI fixes.

### Notes

- Existing admin credentials remain usable after the auth hardening migration until they are changed from the admin security page.
- The current hardening focus has been runtime and import reliability first, with admin/security changes kept separate from the main operational workflow.
