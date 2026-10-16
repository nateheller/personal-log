from hashlib import sha256
import sqlite3
from uuid import uuid4

import pytest

from personal_log.recording.fake import AudioFormat, FakeMicrophone
from personal_log.recording.ipc import command, status
from personal_log.storage import database
from personal_log.storage.segments import PersistenceError, SegmentStore


def setup_writer(store):
    sid, interval = store.begin_session(audio_format=AudioFormat(sample_rate=10))
    writer = store.writer(sid, interval)
    fake = FakeMicrophone(AudioFormat(sample_rate=10))
    fake.start()
    writer.append(fake.read(5))
    message = command("finish", str(uuid4()), sid, 1)
    response = status("finished", "ack", session_id=sid, command_id=message["message_id"], sequence=1)
    return sid, writer, message, response


@pytest.mark.parametrize("rejected_table", ["processing_jobs", "recorder_commands"])
def test_finish_job_receipt_and_events_roll_back_together(external, checkout, rejected_table):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer, message, response = setup_writer(store)
        store.db.execute(f"CREATE TRIGGER reject_insert BEFORE INSERT ON {rejected_table} BEGIN SELECT RAISE(ABORT,'synthetic rejection'); END")
        with pytest.raises(PersistenceError):
            writer.finish(enqueue=True, receipt=lambda: store.remember_command(message, response))
        assert store.db.execute("SELECT count(*) FROM processing_jobs").fetchone()[0] == 0
        assert store.db.execute("SELECT count(*) FROM recorder_commands").fetchone()[0] == 0
        assert store.db.execute("SELECT count(*) FROM lifecycle_events WHERE state IN ('finalizing','finished')").fetchone()[0] == 0
        assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "interrupted"
        assert store.db.execute("SELECT count(*) FROM audio_segments WHERE state='finalized'").fetchone()[0] == 1


def test_finish_guard_rolls_back_job_and_receipt_without_changing_audio(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer, message, response = setup_writer(store)

        def guard():
            raise PersistenceError("Synthetic interruption at commit boundary")

        with pytest.raises(PersistenceError):
            writer.finish(enqueue=True, receipt=lambda: store.remember_command(message, response), guard=guard)
        assert store.db.execute("SELECT count(*) FROM processing_jobs").fetchone()[0] == 0
        assert store.db.execute("SELECT count(*) FROM recorder_commands").fetchone()[0] == 0
        assert store.db.execute("SELECT count(*) FROM lifecycle_events WHERE state='finished'").fetchone()[0] == 0
        store.recover()
        assert store.segments(sid)[0]["frame_count"] == 5


def test_finish_ack_can_skip_full_session_rehash(external, checkout, monkeypatch):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        _, writer, _, _ = setup_writer(store)

        def unexpected(*args):
            raise AssertionError("Expensive effective-reference query during acknowledgment")

        monkeypatch.setattr(store, "segments", unexpected)
        assert writer.finish(enqueue=True, return_references=False) == []
        assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "finished"
        assert store.db.execute("SELECT count(*) FROM processing_jobs").fetchone()[0] == 1


def test_command_receipt_is_immutable_and_enqueue_is_idempotent(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer, message, response = setup_writer(store)
        writer.finish(enqueue=True, receipt=lambda: store.remember_command(message, response))
        original = tuple(store.db.execute("SELECT * FROM recorder_commands").fetchone())
        for sql in ("DELETE FROM recorder_commands", "UPDATE recorder_commands SET sequence=2", "INSERT OR REPLACE INTO recorder_commands SELECT * FROM recorder_commands"):
            with pytest.raises(sqlite3.IntegrityError):
                store.db.execute(sql)
        assert tuple(store.db.execute("SELECT * FROM recorder_commands").fetchone()) == original
        with store.transaction("repeat_enqueue"):
            store.enqueue(sid)
        assert store.db.execute("SELECT count(*) FROM processing_jobs").fetchone()[0] == 1


def test_v3_migration_failure_preserves_complete_v2(external, monkeypatch):
    db = sqlite3.connect(external / "v2.sqlite3", isolation_level=None)
    for version in (1, 2):
        sql = database._migration_sql(version)
        digest = sha256(sql.encode()).hexdigest()
        db.executescript("BEGIN;" + sql + f"INSERT INTO schema_migrations VALUES({version},'{digest}'); PRAGMA user_version={version}; COMMIT;")
    original = database._migration_sql
    monkeypatch.setattr(database, "_migration_sql", lambda version=1: original(version) + ("\nINVALID SQL;" if version == 3 else ""))
    with pytest.raises(sqlite3.OperationalError):
        database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 2
    assert db.execute("SELECT name FROM sqlite_master WHERE name='recorder_commands'").fetchall() == []
    monkeypatch.setattr(database, "_migration_sql", original)
    database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == database.SCHEMA_VERSION
    db.close()
