"""Numbered metadata migrations and configured connections; no job executor."""

from hashlib import sha256
from importlib.resources import files
from pathlib import Path
import sqlite3

from .paths import data_file, prepare_data_root


SCHEMA_VERSION = 3


def _migration_sql(version: int = 1) -> str:
    names = {1: "001_recording.sql", 2: "002_segment_persistence.sql", 3: "003_recorder_commands.sql"}
    return files(__package__).joinpath("migrations/" + names[version]).read_text(encoding="utf-8")


def _inspect_schema(connection: sqlite3.Connection) -> int:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if 0 < version <= SCHEMA_VERSION:
        rows = connection.execute("SELECT version, sha256 FROM schema_migrations ORDER BY version").fetchall()
        expected = [(n, sha256(_migration_sql(n).encode("utf-8")).hexdigest()) for n in range(1, version + 1)]
        if rows != expected:
            raise RuntimeError("Migration history differs from this application")
        return version
    if version != 0:
        raise RuntimeError(f"Unsupported database schema version: {version}")
    if connection.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchone():
        raise RuntimeError("Refusing to migrate an unrecognized nonempty database")
    return version


def migrate(connection: sqlite3.Connection) -> None:
    """Commit DDL, checksum, and version together; reject unknown schemas.

    Requires an idle connection. Failed DDL or process termination leaves the
    complete old or new schema, never an advanced version with partial tables.
    """
    if connection.in_transaction:
        raise RuntimeError("Migration requires an idle connection")
    version = _inspect_schema(connection)
    for number in range(version + 1, SCHEMA_VERSION + 1):
        sql = _migration_sql(number)
        digest = sha256(sql.encode("utf-8")).hexdigest()
        try:
            connection.executescript(
                "BEGIN IMMEDIATE;\n" + sql
                + f"\nINSERT INTO schema_migrations VALUES ({number}, '{digest}');"
                + f"\nPRAGMA user_version = {number};\nCOMMIT;"
            )
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise


def open_database(*, repository: Path, root: Path | None = None) -> sqlite3.Connection:
    root = prepare_data_root(repository=repository, root=root)
    # SQLite opens these siblings itself; reject existing redirected sidecars too.
    for suffix in ("", "-wal", "-shm", "-journal"):
        data_file(root, "recording.sqlite3" + suffix)
    connection = sqlite3.connect(data_file(root, "recording.sqlite3"), isolation_level=None, timeout=2.0)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        # Inspect version/history before changing persistence settings.
        _inspect_schema(connection)
        if connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] != "wal":
            raise RuntimeError("WAL mode unavailable")
        connection.execute("PRAGMA synchronous = FULL")
        migrate(connection)
        return connection
    except BaseException:
        connection.close()
        raise
