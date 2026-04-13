# Import Rules: DAT + NestComparison Canonicalization

This document defines how NEWTRACKER imports data and, critically, how it resolves **duplicate DAT filenames** so scanner behavior is stable when users only provide a DAT name.

> Scope: This guide describes actual behavior in `Application/src/newtracker/importer.py` and related scan lookup usage in `ui_state.py`.

---

## Goals

1. Ensure one canonical DB record per DAT filename.
2. Make scan-time lookup deterministic (`barcode_filename` only).
3. Prevent stale parse results after parser changes.
4. Keep expected/scanned part counts consistent across sessions.

---

## Core contract (important)

- Users scan **DAT filename**, not a filesystem path.
- Scanner loads parts by `barcode_filename` from `resolved_nest_parts`.
- Duplicate file resolution is done **during import**, not during scan.

### Practical meaning

For a filename like `LBDLCXDW.DAT`, the database must already contain the best canonical interpretation before any operator scans it.

---

## Data flow overview

### 1) File discovery

`import_test_data(root)` scans recursively under the provided root and classifies files as:

- `amada_dat` (`*.dat`)
- `nest_comparison` (`NestComparison.csv`)

Only those types are imported.

### 2) NestComparison import first

`nest_comparison` files are imported before DAT files so `part_attributes` exists when scoring duplicate DAT candidates.

### 3) DAT grouping by filename

All DAT files are grouped by `path.name.upper()` (barcode filename key).

Example group:

- `.../EMK1Test/LBDLCXHK.DAT`
- `.../Laser/LBDLCXHK.DAT`

### 4) Canonical candidate selection (duplicate handling)

For each filename group, importer parses each candidate and scores it.

Current ranking in `_select_best_dat_candidate(...)` (highest wins):

1. `match_count`: how many parsed part numbers exist in `part_attributes`
2. `total_quantity`: sum of `quantity_nested`
3. `unique_parts`: distinct parsed part numbers
4. `part_rows`: parsed part row count
5. deterministic path tie-break (`str(path).casefold()`, descending via tuple sort)

Then importer writes only the selected candidate to canonical tables (`program_nests`, `nest_parts`) for that barcode filename.

### 5) Canonical write semantics

`import_dat_file(...)` ensures one active source per barcode filename by:

- deleting prior rows by same source path
- deleting rows with same `barcode_filename` but different source path
- inserting new canonical row + its parts

Net effect: one active `program_nests` row per `barcode_filename`.

### 6) Resolved table rebuild

After import, `rebuild_resolved_nest_parts(...)` recreates scanner-facing rows used by UI.

Scanner reads this table, not raw filesystem.

---

## Reprocessing rules (stale-data prevention)

File reprocessing check is in `should_process_file(...)`.

A file is reprocessed when:

- file size changed, or
- modified time changed, or
- stored fingerprint differs from expected fingerprint.

### Fingerprint design

`file_content_fingerprint(...)` stores:

- regular files: plain SHA-256
- DAT files: `sha256|parser=<AMADA_DAT_PARSER_VERSION>`

This allows forced refresh of unchanged DAT files when parser behavior changes.

### Parser version knob

`AMADA_DAT_PARSER_VERSION` in `importer.py` should be bumped whenever DAT parse semantics change in a way that affects persisted `nest_parts`/`resolved_nest_parts`.

---

## Scan-time behavior (must remain simple)

In `ui_state.py`, `load_expected_parts(dat_name)` runs:

```sql
SELECT ...
FROM resolved_nest_parts
WHERE barcode_filename = ?
```

No folder checks, no duplicate competition, no candidate scoring at scan-time.

This is intentional.

---

## Database tables involved

- `processed_files`
  - import bookkeeping (`file_path`, size/mtime, fingerprint hash, status/errors)
- `program_nests`
  - one canonical row per DAT filename (`barcode_filename` unique)
- `nest_parts`
  - canonical parsed part rows for selected DAT source
- `part_attributes`
  - attributes from `NestComparison.csv`; used by candidate scoring + enrichment
- `resolved_nest_parts`
  - scanner-facing expected rows

---

## Duplicate filename policy (human-readable)

When multiple physical files share the same DAT filename:

- choose the candidate that best matches known part attributes and strongest part evidence,
- write only that result as canonical,
- scanner uses only canonical DB rows by filename.

This guarantees users can scan by DAT name without needing path context.

---

## Troubleshooting checklist

If counts look wrong for a DAT filename:

1. Confirm scanner-facing rows:
   - query `resolved_nest_parts` by `barcode_filename`.
2. Confirm canonical nest row:
   - query `program_nests` by `barcode_filename`.
3. Confirm canonical part rows:
   - query `nest_parts` via `nest_id` from `program_nests`.
4. Confirm `processed_files` status for relevant DAT and `NestComparison.csv` files.
5. If parser logic changed recently:
   - bump `AMADA_DAT_PARSER_VERSION`
   - rerun import.

---

## Rules for future changes

1. **Do not** add scan-time folder/path logic.
2. Keep duplicate resolution in import phase only.
3. If changing candidate ranking, update this document and tests/validation queries.
4. If DAT parser changes output semantics, bump `AMADA_DAT_PARSER_VERSION`.
5. Preserve one-row-per-`barcode_filename` invariant in `program_nests`.

---

## Known limitations

- Tie-break fallback ultimately depends on deterministic path sort when primary metrics tie.
- Ranking quality depends on `part_attributes` coverage quality from `NestComparison.csv`.

If better business-specific tie-breakers are needed (date code, machine family, project), add them explicitly to scoring.

---

## Quick glossary

- **Canonical DAT**: the selected physical file used to represent a DAT filename in DB.
- **Barcode filename**: DAT name string (e.g., `LBDLCXDX.DAT`) used for scan lookup.
- **Expected scans**: sum of `quantity_nested` in `resolved_nest_parts` for a barcode filename.
- **Unique parts**: distinct `part_number` values for that barcode filename.
