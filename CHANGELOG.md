# Changelog

This file tracks notable changes made to the NEWTRACKER application during active development.

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
