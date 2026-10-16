from itertools import product
import sqlite3
import subprocess
import sys
from uuid import UUID

import pytest

from personal_log.recording.contracts import State, TRANSITIONS
from personal_log.storage import database


NOW = "2026-10-15T12:00:00Z"
ENTRY, SESSION, INTERVAL, SEGMENT, JOB, EVENT = [str(UUID(int=n)) for n in range(1, 7)]


def insert(db, table, **fields):
    # Table/column identifiers are fixed test literals, never external input.
    db.execute(f"INSERT INTO {table} ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})", tuple(fields.values()))


def seed(db):
    insert(db, "journal_entries", entry_id=ENTRY, created_at=NOW, display_timezone="America/Los_Angeles")
    insert(db, "recording_sessions", session_id=SESSION, entry_id=ENTRY, created_at=NOW, state="starting")
    insert(db, "capture_intervals", interval_id=INTERVAL, session_id=SESSION, sequence=0,
           started_at=NOW, device_id="synthetic", sample_rate=48000, channels=1, sample_width=2)


@pytest.fixture
def db(external, checkout):
    connection = database.open_database(repository=checkout, root=external / "data")
    seed(connection)
    yield connection
    connection.close()


def test_schema_reopen_and_durability_settings(external, checkout):
    root = external / "data"
    db = database.open_database(repository=checkout, root=root)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 1
    assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    seed(db)
    before = db.execute("SELECT * FROM schema_migrations").fetchall()
    db.close()
    db = database.open_database(repository=checkout, root=root)
    assert db.execute("SELECT * FROM schema_migrations").fetchall() == before
    assert db.execute("SELECT count(*) FROM recording_sessions").fetchone()[0] == 1
    db.close()


def test_supplemental_session_uses_same_entry(db):
    insert(db, "recording_sessions", session_id=str(UUID(int=99)), entry_id=ENTRY,
           created_at="2026-11-15T12:00:00Z", state="starting")
    assert db.execute("SELECT count(*) FROM recording_sessions WHERE entry_id=?", (ENTRY,)).fetchone()[0] == 2


@pytest.mark.parametrize("current,target", list(product(State, repeat=2)))
def test_sql_state_transitions_match_contract(db, current, target):
    # Establish each starting condition without weakening the tested update trigger.
    db.execute("DROP TRIGGER session_initial_state")
    db.execute("DELETE FROM capture_intervals")
    db.execute("DELETE FROM recording_sessions")
    insert(db, "recording_sessions", session_id=SESSION, entry_id=ENTRY,
           created_at=NOW, state=current.value, interruption_code="recorder_crash")
    if target == current or target in TRANSITIONS[current]:
        db.execute("UPDATE recording_sessions SET state=?", (target.value,))
    else:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE recording_sessions SET state=?", (target.value,))


@pytest.mark.parametrize("field,value", [
    ("session_id", "x" * 36), ("entry_id", str(UUID(int=90))),
    ("state", "finished"), ("created_at", "not a date"),
    ("created_at", "2026-10-15T12:00:00+00:00"), ("schema_version", 2),
    ("input_revision", -1),
])
def test_session_constraints(db, field, value):
    fields = dict(session_id=str(UUID(int=99)), entry_id=ENTRY, created_at=NOW, state="starting")
    fields[field] = value
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "recording_sessions", **fields)


def segment_fields(**overrides):
    fields = dict(segment_id=SEGMENT, session_id=SESSION, interval_id=INTERVAL,
                  sequence=0, created_at=NOW, start_frame=0,
                  relative_path=f"sessions/{SESSION}/segments/{SEGMENT}.wav", state="writing")
    fields.update(overrides)
    return fields


@pytest.mark.parametrize("path", ["/audio.wav", "../audio.wav", "a/../x", "a/./x", "C:/x", "a\\b", "a//b", "a/"])
def test_segment_paths(db, path):
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "audio_segments", **segment_fields(relative_path=path))


def test_segment_integrity_and_ordering(db):
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "audio_segments", **segment_fields(state="finalized"))
    insert(db, "audio_segments", **segment_fields(state="finalized", sha256="a" * 64, frame_count=10, byte_length=64))
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "audio_segments", **segment_fields(segment_id=str(UUID(int=99)), relative_path="other.wav"))
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("DELETE FROM capture_intervals")


def test_segment_interval_cannot_belong_to_another_session(db):
    other = str(UUID(int=99))
    insert(db, "recording_sessions", session_id=other, entry_id=ENTRY, created_at=NOW, state="starting")
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "audio_segments", **segment_fields(session_id=other))


def test_only_one_open_interval_and_unique_order(db):
    fields = dict(interval_id=str(UUID(int=99)), session_id=SESSION, sequence=1,
                  started_at=NOW, device_id="synthetic", sample_rate=48000, channels=1, sample_width=2)
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "capture_intervals", **fields)
    db.execute("UPDATE capture_intervals SET ended_at=?, end_reason='pause'", (NOW,))
    insert(db, "capture_intervals", **fields)
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE capture_intervals SET sequence=0 WHERE interval_id=?", (fields["interval_id"],))


def test_append_only_events(db):
    insert(db, "lifecycle_events", event_id=EVENT, session_id=SESSION, sequence=0,
           created_at=NOW, state="starting", code="state_changed")
    for sql in ("UPDATE lifecycle_events SET sequence=1", "DELETE FROM lifecycle_events"):
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(sql)
    for event_id in (EVENT, str(UUID(int=99))):
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("INSERT OR REPLACE INTO lifecycle_events (event_id,session_id,sequence,created_at,state,code) VALUES (?,?,0,?,'recording','state_changed')", (event_id, SESSION, NOW))
    assert db.execute("SELECT state FROM lifecycle_events").fetchall() == [("starting",)]


@pytest.mark.parametrize("failure", ["duplicate_sequence", "invalid_code"])
def test_failed_lifecycle_event_rolls_back_state_change(db, failure):
    insert(db, "lifecycle_events", event_id=EVENT, session_id=SESSION, sequence=0,
           created_at=NOW, state="starting", code="state_changed")
    with pytest.raises(sqlite3.IntegrityError):
        # open_database uses autocommit: the context manager alone does not BEGIN.
        with db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE recording_sessions SET state='recording' WHERE session_id=?", (SESSION,))
            assert db.execute("SELECT state FROM recording_sessions WHERE session_id=?", (SESSION,)).fetchone()[0] == "recording"
            insert(db, "lifecycle_events", event_id=str(UUID(int=99)), session_id=SESSION,
                   sequence=0 if failure == "duplicate_sequence" else 1,
                   created_at=NOW, state="recording",
                   code="state_changed" if failure == "duplicate_sequence" else "invalid")
    assert not db.in_transaction
    # A fresh connection verifies committed state, not just the writer's view.
    path = db.execute("PRAGMA database_list").fetchone()[2]
    observer = sqlite3.connect(path)
    try:
        assert observer.execute("SELECT state FROM recording_sessions WHERE session_id=?", (SESSION,)).fetchone() == ("starting",)
        assert observer.execute("SELECT event_id, state FROM lifecycle_events").fetchall() == [(EVENT, "starting")]
    finally:
        observer.close()


def test_state_and_lifecycle_event_commit_together(db):
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE recording_sessions SET state='recording' WHERE session_id=?", (SESSION,))
        insert(db, "lifecycle_events", event_id=EVENT, session_id=SESSION, sequence=0,
               created_at=NOW, state="recording", code="state_changed")
    assert not db.in_transaction
    path = db.execute("PRAGMA database_list").fetchone()[2]
    observer = sqlite3.connect(path)
    try:
        assert observer.execute("SELECT s.state, e.state FROM recording_sessions s JOIN lifecycle_events e ON e.session_id=s.session_id WHERE s.session_id=?", (SESSION,)).fetchall() == [("recording", "recording")]
    finally:
        observer.close()


def job_fields(**overrides):
    fields = dict(job_id=JOB, session_id=SESSION, created_at=NOW, job_type="process_session",
                  input_revision=0, pipeline_version="future-v1", state="blocked")
    fields.update(overrides)
    return fields


def test_job_uniqueness_and_leases(db):
    insert(db, "processing_jobs", **job_fields())
    with pytest.raises(sqlite3.IntegrityError):
        insert(db, "processing_jobs", **job_fields(job_id=str(UUID(int=99))))
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE processing_jobs SET state='running'")
    db.execute("UPDATE processing_jobs SET state='running', lease_owner='synthetic', lease_expires_at=?", (NOW,))
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE processing_jobs SET state='succeeded'")
    db.execute("UPDATE processing_jobs SET state='succeeded', lease_owner=NULL, lease_expires_at=NULL")


def test_failed_migration_rolls_back_ddl_and_version(external, monkeypatch):
    db = sqlite3.connect(external / "failed.sqlite3", isolation_level=None)
    original = database._migration_sql()
    monkeypatch.setattr(database, "_migration_sql", lambda: original + "\nINVALID SQL;")
    with pytest.raises(sqlite3.OperationalError):
        database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 0
    assert db.execute("SELECT name FROM sqlite_master").fetchall() == []
    monkeypatch.setattr(database, "_migration_sql", lambda: original)
    database.migrate(db)
    db.close()


def test_abrupt_process_exit_during_migration_is_recoverable(external):
    path = external / "interrupted.sqlite3"
    script = """
import os, sqlite3, sys
from personal_log.storage import database
db = sqlite3.connect(sys.argv[1], isolation_level=None)
db.execute('PRAGMA journal_mode=WAL')
db.execute('PRAGMA synchronous=FULL')
db.create_function('crash', 0, lambda: os._exit(77))
original = database._migration_sql()
database._migration_sql = lambda: original + '\\nSELECT crash();'
database.migrate(db)
"""
    result = subprocess.run([sys.executable, "-c", script, str(path)], capture_output=True, text=True, timeout=20)
    assert result.returncode == 77, result.stderr
    db = sqlite3.connect(path, isolation_level=None)
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert db.execute("PRAGMA user_version").fetchone()[0] == 0
    assert db.execute("SELECT name FROM sqlite_master").fetchall() == []
    database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 1
    db.close()


def test_newer_unknown_and_drifted_databases_rejected(external):
    db = sqlite3.connect(external / "unknown.sqlite3", isolation_level=None)
    db.execute("PRAGMA user_version=99")
    with pytest.raises(RuntimeError, match="Unsupported"):
        database.migrate(db)
    db.execute("PRAGMA user_version=0")
    db.execute("CREATE TABLE unknown (id INTEGER)")
    with pytest.raises(RuntimeError, match="unrecognized"):
        database.migrate(db)
    db.execute("DROP TABLE unknown")
    database.migrate(db)
    db.execute("UPDATE schema_migrations SET sha256=?", ("0" * 64,))
    with pytest.raises(RuntimeError, match="history"):
        database.migrate(db)
    db.close()


@pytest.mark.parametrize("kind", ["newer", "unknown", "drifted"])
def test_rejected_database_keeps_existing_journal_mode(external, checkout, kind):
    root = external / "data"
    root.mkdir()
    db = sqlite3.connect(root / "recording.sqlite3", isolation_level=None)
    if kind == "newer":
        db.execute("PRAGMA user_version=99")
    elif kind == "unknown":
        db.execute("CREATE TABLE unknown (id INTEGER)")
    else:
        database.migrate(db)
        db.execute("UPDATE schema_migrations SET sha256=?", ("0" * 64,))
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    db.close()
    with pytest.raises(RuntimeError):
        database.open_database(repository=checkout, root=root)
    db = sqlite3.connect(root / "recording.sqlite3")
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    db.close()
