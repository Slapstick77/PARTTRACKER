from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
import re
from typing import Any


class SqlServerDriverMissingError(RuntimeError):
    pass


def translate_qmark_to_pyformat(operation: str) -> str:
    translated: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0
    length = len(operation)

    while index < length:
        char = operation[index]
        next_char = operation[index + 1] if index + 1 < length else ""

        if char == "'" and not in_double_quote:
            translated.append(char)
            if in_single_quote and next_char == "'":
                translated.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue

        if char == '"' and not in_single_quote:
            translated.append(char)
            if in_double_quote and next_char == '"':
                translated.append(next_char)
                index += 2
                continue
            in_double_quote = not in_double_quote
            index += 1
            continue

        if char == "?" and not in_single_quote and not in_double_quote:
            translated.append("%s")
        else:
            translated.append(char)
        index += 1

    return "".join(translated)


def split_sql_statements(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0
    length = len(script)

    while index < length:
        char = script[index]
        next_char = script[index + 1] if index + 1 < length else ""

        if char == "'" and not in_double_quote:
            current.append(char)
            if in_single_quote and next_char == "'":
                current.append(next_char)
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue

        if char == '"' and not in_single_quote:
            current.append(char)
            if in_double_quote and next_char == '"':
                current.append(next_char)
                index += 2
                continue
            in_double_quote = not in_double_quote
            index += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _load_pytds() -> Any:
    try:
        import pytds  # type: ignore
    except ImportError as exc:
        raise SqlServerDriverMissingError(
            "SQL Server backend requires the 'python-tds' package. Install Application/requirements.txt before using NEWTRACKER_DB_BACKEND=sqlserver."
        ) from exc
    return pytds


def _load_default_ca_file() -> str:
    try:
        import certifi
    except ImportError as exc:
        raise SqlServerDriverMissingError(
            "SQL Server backend requires the 'certifi' package so TLS certificates can be validated."
        ) from exc
    return certifi.where()


def _ensure_tls_support() -> None:
    try:
        import OpenSSL.SSL  # type: ignore  # noqa: F401
    except ImportError as exc:
        raise SqlServerDriverMissingError(
            "SQL Server backend requires 'pyOpenSSL' so TLS can be enabled for Azure SQL connections."
        ) from exc


class CompatRow(Sequence[Any]):
    def __init__(self, column_names: Sequence[str], values: Sequence[Any]) -> None:
        self._column_names = tuple(column_names)
        self._values = tuple(values)
        self._mapping = {name: self._values[index] for index, name in enumerate(self._column_names)}

    def __getitem__(self, key: int | slice | str) -> Any:
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._mapping[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> tuple[str, ...]:
        return self._column_names

    def values(self) -> tuple[Any, ...]:
        return self._values

    def items(self) -> tuple[tuple[str, Any], ...]:
        return tuple((name, self._mapping[name]) for name in self._column_names)

    def get(self, key: str, default: Any = None) -> Any:
        return self._mapping.get(key, default)

    def __repr__(self) -> str:
        payload = ", ".join(f"{name}={self._mapping[name]!r}" for name in self._column_names)
        return f"CompatRow({payload})"


def _extract_column_names(description: Sequence[Sequence[Any]] | None) -> tuple[str, ...]:
    if not description:
        return ()
    return tuple(str(column[0]) for column in description)


class SqlServerCursorWrapper:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self.lastrowid: int | None = None

    @property
    def connection(self) -> Any:
        return self._cursor.connection

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    @property
    def messages(self) -> Any:
        return self._cursor.messages

    def _wrap_row(self, row: Any) -> Any:
        if row is None:
            return None
        if isinstance(row, CompatRow):
            return row
        if isinstance(row, dict):
            return CompatRow(tuple(str(key) for key in row.keys()), tuple(row.values()))
        column_names = _extract_column_names(self._cursor.description)
        if not column_names:
            return row
        return CompatRow(column_names, tuple(row))

    def _run_insert_with_identity(
        self,
        operation: str,
        params: Sequence[Any] | dict[str, Any] | None,
    ) -> SqlServerCursorWrapper:
        translated = translate_qmark_to_pyformat(operation).rstrip().rstrip(";")
        batch = (
            f"SET NOCOUNT ON; {translated}; "
            "SELECT CAST(SCOPE_IDENTITY() AS int) AS __newtracker_lastrowid;"
        )
        self._cursor.execute(batch, params)
        while True:
            if self._cursor.description:
                row = self._cursor.fetchone()
                wrapped = self._wrap_row(row)
                if wrapped is not None:
                    self.lastrowid = int(wrapped[0]) if wrapped[0] is not None else None
                break
            if not self._cursor.nextset():
                break
        while self._cursor.nextset():
            pass
        return self

    def execute(
        self,
        operation: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
    ) -> SqlServerCursorWrapper:
        self.lastrowid = None
        if re.match(r"^\s*INSERT\b", operation, flags=re.IGNORECASE):
            return self._run_insert_with_identity(operation, params)

        translated = translate_qmark_to_pyformat(operation)
        self._cursor.execute(translated, params)
        return self

    def executemany(self, operation: str, params_seq: Iterable[Sequence[Any] | dict[str, Any]]) -> SqlServerCursorWrapper:
        translated = translate_qmark_to_pyformat(operation)
        self._cursor.executemany(translated, params_seq)
        self.lastrowid = None
        return self

    def fetchone(self) -> Any:
        return self._wrap_row(self._cursor.fetchone())

    def fetchmany(self, size: int | None = None) -> list[Any]:
        rows = self._cursor.fetchmany(size) if size is not None else self._cursor.fetchmany()
        return [self._wrap_row(row) for row in rows]

    def fetchall(self) -> list[Any]:
        return [self._wrap_row(row) for row in self._cursor.fetchall()]

    def nextset(self) -> bool | None:
        return self._cursor.nextset()

    def close(self) -> None:
        self._cursor.close()

    def __enter__(self) -> SqlServerCursorWrapper:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __iter__(self) -> Iterator[Any]:
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row


class SqlServerConnectionWrapper:
    def __init__(self, connection: Any, *, reconnect_kwargs: dict | None = None) -> None:
        self._connection = connection
        self._reconnect_kwargs = reconnect_kwargs or {}

    def ensure_connected(self) -> None:
        """Ping the connection and silently reconnect if Azure SQL has dropped it."""
        if not self._reconnect_kwargs:
            return
        try:
            cur = self._connection.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception:
            try:
                self._connection.close()
            except Exception:
                pass
            pytds = _load_pytds()
            self._connection = pytds.connect(**self._reconnect_kwargs)

    def cursor(self) -> SqlServerCursorWrapper:
        return SqlServerCursorWrapper(self._connection.cursor())

    def execute(
        self,
        operation: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
    ) -> SqlServerCursorWrapper:
        cursor = self.cursor()
        return cursor.execute(operation, params)

    def executemany(self, operation: str, params_seq: Iterable[Sequence[Any] | dict[str, Any]]) -> SqlServerCursorWrapper:
        cursor = self.cursor()
        return cursor.executemany(operation, params_seq)

    def executescript(self, script: str) -> SqlServerCursorWrapper | None:
        last_cursor: SqlServerCursorWrapper | None = None
        for statement in split_sql_statements(script):
            last_cursor = self.execute(statement)
        return last_cursor

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SqlServerConnectionWrapper:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                try:
                    self.rollback()
                except Exception:
                    pass
        else:
            try:
                self.rollback()
            except Exception:
                pass
        self.close()


def connect_sqlserver(
    *,
    server: str,
    database: str,
    username: str,
    password: str,
    port: int,
    cafile: str | None = None,
) -> SqlServerConnectionWrapper:
    pytds = _load_pytds()
    _ensure_tls_support()
    reconnect_kwargs = dict(
        dsn=server,
        database=database,
        user=username,
        password=password,
        port=port,
        login_timeout=15,
        timeout=30,
        autocommit=False,
        use_mars=True,
        bytes_to_unicode=True,
        appname="NEWTRACKER",
        cafile=cafile or _load_default_ca_file(),
        validate_host=True,
        enc_login_only=False,
        pooling=False,
    )
    connection = pytds.connect(**reconnect_kwargs)
    return SqlServerConnectionWrapper(connection, reconnect_kwargs=reconnect_kwargs)