"""SQLite schema bootstrap only; no recording writer or job executor yet."""

from hashlib import sha256
from importlib.resources import files
from pathlib import Path
import sqlite3

from .paths import data_file, prepare_data_root


SCHEMA_VERSION = 1


def _migration_sql() -> str:
    return files(__package__).joinpath("migrations/001_recording.sql").read_text(encoding="utf-8")


def _inspect_schema(connection: sqlite3.Connection, digest: str) -> int:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version == SCHEMA_VERSION:
        rows = connection.execute("SELECT version, sha256 FROM schema_migrations ORDER BY version").fetchall()
        if rows != [(SCHEMA_VERSION, digest)]:
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
    sql = _migration_sql()
    digest = sha256(sql.encode("utf-8")).hexdigest()
    if _inspect_schema(connection, digest) == SCHEMA_VERSION:
        return
    try:
        connection.executescript(
            "BEGIN IMMEDIATE;\n" + sql
            + f"\nINSERT INTO schema_migrations VALUES (1, '{digest}');"
            + "\nPRAGMA user_version = 1;\nCOMMIT;"
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
        _inspect_schema(connection, sha256(_migration_sql().encode("utf-8")).hexdigest())
        if connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] != "wal":
            raise RuntimeError("WAL mode unavailable")
        connection.execute("PRAGMA synchronous = FULL")
        migrate(connection)
        return connection
    except BaseException:
        connection.close()
        raise
