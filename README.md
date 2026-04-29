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

## Azure Web App Setup

NEWTRACKER should be deployed to Azure App Service as a Linux Python app.
Python on Windows App Service is not supported.

This repository now includes the two deployment files App Service expects when you deploy from the repo root:

- `requirements.txt` at the repo root, which points to `Application/requirements.txt`
- `startup.txt` at the repo root, which starts Gunicorn against `Application/wsgi.py`

Recommended App Service settings:

- OS: `Linux`
- Runtime stack: `Python 3.12`
- Startup Command: `startup.txt`
- App setting `SCM_DO_BUILD_DURING_DEPLOYMENT`: `1`
- App setting `NEWTRACKER_DB_BACKEND`: `sqlserver`
- App setting `NEWTRACKER_SQLSERVER_SERVER`: `newtracker-sql-4821.database.windows.net`
- App setting `NEWTRACKER_SQLSERVER_DATABASE`: `newtracker-db`
- App setting `NEWTRACKER_SQLSERVER_USERNAME`: `newtrackeradmin`
- App setting `NEWTRACKER_SQLSERVER_PASSWORD`: your SQL password
- App setting `NEWTRACKER_SQLSERVER_PORT`: `1433`
- App setting `NEWTRACKER_DATA_DIR`: `/home/site/newtracker-data`

After the app is deployed, initialize the database schema once and then run an import so the app has data:

```bash
python Application/scripts/init_db.py
python Application/scripts/import_test_data.py
```

If you do not want the sample import on Azure, skip `import_test_data.py` and use the admin import flow after deployment instead.

## What is not pulled from git

These are intentionally local and will be recreated per machine as needed:

- `.venv/`
- `Application/data/newtracker.db`
- `Application/data/ui_sessions/`
- `Application/data/barcode_sheets/`
- `Application/data/admin_settings.json`

## More detail

For application-specific behavior and workflow details, see `Application/README.md`.