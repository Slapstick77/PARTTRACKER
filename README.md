# NEWTRACKER

NEWTRACKER is a local Flask and SQLite application for importing manufacturing data, tracking part scanning progress, and generating barcode sheets.

## Fresh machine setup

This repository does not commit the virtual environment or generated runtime data. After cloning on another machine or VM, create a local Python environment and install the committed dependencies from `Application/requirements.txt`.

Python 3.10 or newer is required. The current workspace is using Python 3.12.

On Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r Application\requirements.txt
```

## Run the app

To start the local UI:

```powershell
.\.venv\Scripts\python.exe Application\scripts\run_clean_ui.py
```

Then open `http://127.0.0.1:5000/`.

## Optional bootstrap commands

Create the SQLite schema explicitly:

```powershell
.\.venv\Scripts\python.exe Application\scripts\init_db.py
```

Import the sample data included in this repository:

```powershell
.\.venv\Scripts\python.exe Application\scripts\import_test_data.py
```

Generate barcode sheets:

```powershell
.\.venv\Scripts\python.exe Application\scripts\generate_barcodes.py
```

## What is not pulled from git

These are intentionally local and will be recreated per machine as needed:

- `.venv/`
- `Application/data/newtracker.db`
- `Application/data/ui_sessions/`
- `Application/data/barcode_sheets/`
- `Application/data/admin_settings.json`

## More detail

For application-specific behavior and workflow details, see `Application/README.md`.