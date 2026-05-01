from __future__ import annotations

from dataclasses import dataclass
import os
import sqlite3
from pathlib import Path
from typing import Any

from .db_sqlite import connect_sqlite
from .db_sqlserver import SqlServerDriverMissingError, connect_sqlserver

APP_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR_ENV_VAR = "NEWTRACKER_DATA_DIR"
DB_BACKEND_ENV_VAR = "NEWTRACKER_DB_BACKEND"
SQLITE_PATH_ENV_VAR = "NEWTRACKER_SQLITE_PATH"
SQLSERVER_SERVER_ENV_VAR = "NEWTRACKER_SQLSERVER_SERVER"
SQLSERVER_DATABASE_ENV_VAR = "NEWTRACKER_SQLSERVER_DATABASE"
SQLSERVER_USERNAME_ENV_VAR = "NEWTRACKER_SQLSERVER_USERNAME"
SQLSERVER_PASSWORD_ENV_VAR = "NEWTRACKER_SQLSERVER_PASSWORD"
SQLSERVER_PORT_ENV_VAR = "NEWTRACKER_SQLSERVER_PORT"
SQLSERVER_CA_PATH_ENV_VAR = "NEWTRACKER_SQLSERVER_CA_PATH"
SUPPORTED_DB_BACKENDS = {"sqlite", "sqlserver"}


class DatabaseConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseSettings:
    backend: str
    sqlite_path: Path | None = None
    sqlserver_server: str = ""
    sqlserver_database: str = ""
    sqlserver_username: str = ""
    sqlserver_password: str = ""
    sqlserver_port: int = 1433
    sqlserver_ca_path: str = ""


def resolve_data_dir() -> Path:
    configured = os.getenv(DATA_DIR_ENV_VAR, "").strip()
    if not configured:
        return APP_ROOT / "data"

    candidate = Path(configured).expanduser()
    if not candidate.is_absolute():
        candidate = APP_ROOT / candidate
    return candidate


def _resolve_sqlite_path() -> Path:
    configured = os.getenv(SQLITE_PATH_ENV_VAR, "").strip()
    if not configured:
        return DATA_DIR / "newtracker.db"

    candidate = Path(configured).expanduser()
    if not candidate.is_absolute():
        candidate = APP_ROOT / candidate
    return candidate


def _resolve_sqlserver_settings() -> DatabaseSettings:
    server = os.getenv(SQLSERVER_SERVER_ENV_VAR, "").strip()
    database = os.getenv(SQLSERVER_DATABASE_ENV_VAR, "").strip()
    username = os.getenv(SQLSERVER_USERNAME_ENV_VAR, "").strip()
    password = os.getenv(SQLSERVER_PASSWORD_ENV_VAR, "")
    port_raw = os.getenv(SQLSERVER_PORT_ENV_VAR, "").strip()
    ca_path = os.getenv(SQLSERVER_CA_PATH_ENV_VAR, "").strip()

    missing: list[str] = []
    if not server:
        missing.append(SQLSERVER_SERVER_ENV_VAR)
    if not database:
        missing.append(SQLSERVER_DATABASE_ENV_VAR)
    if not username:
        missing.append(SQLSERVER_USERNAME_ENV_VAR)
    if not password:
        missing.append(SQLSERVER_PASSWORD_ENV_VAR)
    if missing:
        raise DatabaseConfigurationError(
            "SQL Server backend requires the following environment variables: " + ", ".join(missing)
        )

    if port_raw:
        try:
            port = int(port_raw)
        except ValueError as exc:
            raise DatabaseConfigurationError(
                f"{SQLSERVER_PORT_ENV_VAR} must be an integer when set."
            ) from exc
    else:
        port = 1433

    return DatabaseSettings(
        backend="sqlserver",
        sqlserver_server=server,
        sqlserver_database=database,
        sqlserver_username=username,
        sqlserver_password=password,
        sqlserver_port=port,
        sqlserver_ca_path=ca_path,
    )


def get_database_settings() -> DatabaseSettings:
    backend = os.getenv(DB_BACKEND_ENV_VAR, "sqlite").strip().lower() or "sqlite"
    if backend not in SUPPORTED_DB_BACKENDS:
        supported = ", ".join(sorted(SUPPORTED_DB_BACKENDS))
        raise DatabaseConfigurationError(
            f"Unsupported database backend '{backend}'. Expected one of: {supported}."
        )

    if backend == "sqlite":
        return DatabaseSettings(backend=backend, sqlite_path=_resolve_sqlite_path())

    return _resolve_sqlserver_settings()


def describe_database_target() -> str:
    settings = get_database_settings()
    if settings.backend == "sqlite" and settings.sqlite_path is not None:
        return f"sqlite:{settings.sqlite_path}"
    if settings.backend == "sqlserver":
        return f"sqlserver:{settings.sqlserver_server}:{settings.sqlserver_port}/{settings.sqlserver_database}"
    return settings.backend


DATA_DIR = resolve_data_dir()
DB_PATH = _resolve_sqlite_path()


def get_connection() -> Any:
    settings = get_database_settings()
    if settings.backend == "sqlite" and settings.sqlite_path is not None:
        return connect_sqlite(settings.sqlite_path)

    if settings.backend == "sqlserver":
        try:
            return connect_sqlserver(
                server=settings.sqlserver_server,
                database=settings.sqlserver_database,
                username=settings.sqlserver_username,
                password=settings.sqlserver_password,
                port=settings.sqlserver_port,
                cafile=settings.sqlserver_ca_path or None,
            )
        except SqlServerDriverMissingError as exc:
            raise DatabaseConfigurationError(str(exc)) from exc
        except Exception as exc:
            detail = str(exc).strip()
            suffix = f" Original error: {detail}" if detail else ""
            raise DatabaseConfigurationError(
                "Unable to connect to SQL Server with the configured NEWTRACKER_SQLSERVER_* settings. "
                "Check the server name, database name, username, password, and Azure SQL firewall access."
                f"{suffix}"
            ) from exc

    raise DatabaseConfigurationError(f"Unsupported database backend '{settings.backend}'.")
