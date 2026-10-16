from hashlib import sha256
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import wave

import pytest

from personal_log.recording.fake import AudioBlock, AudioFormat, FakeMicrophone
from personal_log.storage import database
from personal_log.storage.files import DurableIO, artifact_path, inspect_wav, wav_header
from personal_log.storage.segments import PersistenceError, SegmentStore


RATE = 10


def feed(writer, frames, *, capture=None):
    capture = capture or FakeMicrophone(AudioFormat(sample_rate=RATE))
    if not capture.running:
        capture.start()
    while frames:
        block = capture.read(min(frames, capture.format.sample_rate))
        writer.append(block)
        frames -= block.frames
    return capture


def start(store):
    sid, interval = store.begin_session(audio_format=AudioFormat(sample_rate=RATE))
    return sid, store.writer(sid, interval)


def read_pcm(store, references):
    pcm = b""
    for item in references:
        path = artifact_path(store.root, item["relative_path"])
        info = inspect_wav(path, RATE)
        assert (info.frames, info.byte_length, info.sha256) == (item["frame_count"], item["byte_length"], item["sha256"])
        with wave.open(str(path), "rb") as audio:
            pcm += audio.readframes(audio.getnframes())
    return pcm


def snapshot(store):
    names = ("recording_sessions", "capture_intervals", "audio_segments", "segment_storage",
             "segment_recoveries", "recovery_findings", "lifecycle_events")
    return {name: [tuple(row) for row in store.db.execute(f"SELECT * FROM {name} ORDER BY rowid")] for name in names}


class FaultIO(DurableIO):
    def __init__(self, boundary):
        self.boundary = boundary
        self.triggered = False

    def hook(self, boundary):
        if boundary == self.boundary and not self.triggered:
            self.triggered = True
            raise OSError("Synthetic persistence failure at " + boundary)


def test_twenty_second_rotation_and_exact_continuity(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        # One block crosses both a checkpoint and segment boundary.
        capture = feed(writer, 193)
        for size in (10, 10):
            writer.append(capture.read(size))
        feed(writer, 240, capture=capture)
        refs = writer.finish()
        assert [r["frame_count"] for r in refs] == [200, 200, 53]
        assert [r["sequence"] for r in refs] == [0, 1, 2]
        assert [r["start_frame"] for r in refs] == [0, 200, 400]
        expected = FakeMicrophone(AudioFormat(sample_rate=RATE))
        expected.start()
        chunks = []
        for _ in range(45):
            chunks.append(expected.read(10).pcm)
        chunks.append(expected.read(3).pcm)
        assert read_pcm(store, refs) == b"".join(chunks)
        assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "finished"
        before = snapshot(store)
        assert store.recover() == []
        assert snapshot(store) == before


def test_one_second_checkpoints_and_explicit_tail_checkpoint(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        _, writer = start(store)
        feed(writer, 13)
        row = store.db.execute("SELECT accepted_frames,durable_frames FROM segment_storage").fetchone()
        assert tuple(row) == (13, 10)
        writer.checkpoint()
        assert tuple(store.db.execute("SELECT accepted_frames,durable_frames FROM segment_storage").fetchone()) == (13, 13)


@pytest.mark.parametrize("boundary", [
    "segment_intent.before_commit", "segment_intent.committed", "stage.before_create", "stage.created",
    "accepted.before_commit", "accepted.committed", "pcm.before_write", "pcm.written",
    "pcm.before_flush", "pcm.flushed", "checkpoint.before_commit", "checkpoint.committed",
    "wav.created", "wav.written", "wav.flushed", "publication_intent.before_commit",
    "publication_intent.committed", "publication.before", "publication.after",
    "finalized.before_commit", "finalized.committed", "session_finish.before_commit", "session_finish.committed",
])
def test_failure_boundaries_never_acknowledge_saved_and_recover_idempotently(external, checkout, boundary):
    root = external / "data"
    io = FaultIO(boundary)
    with SegmentStore(repository=checkout, root=root, io=io) as store:
        sid, writer = start(store)
        with pytest.raises(PersistenceError):
            feed(writer, 13)
            writer.finish()
        assert io.triggered
        assert writer.closed
    with SegmentStore(repository=checkout, root=root) as store:
        first = store.recover()
        before = snapshot(store)
        artifacts = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        assert store.recover() == first
        assert snapshot(store) == before
        assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == artifacts
        for ref in store.segments(sid):
            inspect_wav(artifact_path(root, ref["relative_path"]), RATE)


class ShortIO(DurableIO):
    def write(self, fd, data):
        return super().write(fd, data[:3])


def test_short_writes_are_completed_without_missing_samples(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data", io=ShortIO()) as store:
        _, writer = start(store)
        capture = FakeMicrophone(AudioFormat(sample_rate=RATE))
        capture.start()
        block = capture.read(9)
        writer.append(block)
        assert read_pcm(store, writer.finish()) == block.pcm


class PartialFailureIO(DurableIO):
    def __init__(self):
        self.calls = 0

    def write(self, fd, data):
        self.calls += 1
        if self.calls == 1:
            return super().write(fd, data[:5])
        raise OSError("Synthetic disk full after partial frame")


def test_partial_frame_is_preserved_and_complete_prefix_recovered(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root, io=PartialFailureIO()) as store:
        sid, writer = start(store)
        with pytest.raises(PersistenceError):
            feed(writer, 10)
        stage = artifact_path(root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        original = stage.read_bytes()
        assert len(original) == 5
    with SegmentStore(repository=checkout, root=root) as store:
        findings = store.recover()
        assert stage.read_bytes() == original
        refs = store.segments(sid)
        assert len(refs) == 1 and refs[0]["frame_count"] == 2
        assert read_pcm(store, refs) == original[:4]
        repair = store.db.execute("SELECT missing_frames,partial_bytes FROM segment_recoveries").fetchone()
        assert tuple(repair) == (8, 1)
        assert {f["code"] for f in findings} >= {"missing_frames", "partial_frame", "capture_tail_unknown"}
        before = snapshot(store)
        store.recover()
        assert snapshot(store) == before


class ZeroWriteIO(DurableIO):
    def write(self, fd, data):
        return 0


class FlushFailureIO(DurableIO):
    def flush(self, fd):
        raise OSError("Synthetic fsync failure")


@pytest.mark.parametrize("io", [ZeroWriteIO, FlushFailureIO])
def test_actual_write_and_flush_errors_fail_closed(external, checkout, io):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root, io=io()) as store:
        _, writer = start(store)
        with pytest.raises(PersistenceError):
            feed(writer, 10)
        assert store.db.execute("SELECT count(*) FROM audio_segments WHERE state='finalized'").fetchone()[0] == 0
        assert store.db.execute("SELECT durable_frames FROM segment_storage").fetchone()[0] == 0


def test_publication_never_overwrites_collision_or_trusts_foreign_wav(external, checkout):
    root = external / "data"

    class CollisionIO(DurableIO):
        target = None

        def publish(self, source, target):
            self.target = target
            target.write_bytes(wav_header(13, RATE) + b"\x01\x02" * 13)
            super().publish(source, target)

    io = CollisionIO()
    with SegmentStore(repository=checkout, root=root, io=io) as store:
        sid, writer = start(store)
        feed(writer, 13)
        with pytest.raises(PersistenceError):
            writer.finish()
        foreign = io.target.read_bytes()
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        assert io.target.read_bytes() == foreign
        refs = store.segments(sid)
        assert refs[0]["recovered"]
        assert read_pcm(store, refs) != b"\x01\x02" * 13


def test_stage_creation_never_overwrites_existing_artifact(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)

        class StageCollisionIO(DurableIO):
            def hook(self, boundary):
                if boundary == "segment_intent.committed":
                    row = store.db.execute("SELECT staging_path FROM segment_storage").fetchone()
                    artifact_path(root, row[0]).write_bytes(b"preserve existing artifact")

        store.io = StageCollisionIO()
        with pytest.raises(PersistenceError):
            feed(writer, 3)
        stage = artifact_path(root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        assert stage.read_bytes() == b"preserve existing artifact"
        store.io = DurableIO()
        store.recover()
        assert stage.read_bytes() == b"preserve existing artifact"
        assert store.segments(sid) == []  # No accepted-frame intent authenticates these bytes.


@pytest.mark.parametrize("damage", ["missing", "truncated", "mutated", "repair_damaged"])
def test_finalized_corruption_preserves_original_evidence_and_blocks_jobs(external, checkout, damage):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        ref = writer.finish()[0]
        original_hash = ref["sha256"]
        path = artifact_path(root, ref["relative_path"])
        if damage == "missing":
            path.unlink()  # Deliberate synthetic failure, external test directory only.
        elif damage == "truncated":
            path.write_bytes(path.read_bytes()[:50])
        else:
            value = bytearray(path.read_bytes())
            value[-1] ^= 1
            path.write_bytes(value)
        damaged = path.read_bytes() if path.exists() else None
        store.db.execute("INSERT INTO processing_jobs (job_id,session_id,created_at,job_type,input_revision,pipeline_version,state) VALUES ('00000000-0000-0000-0000-000000000001',?,?,'process_session',0,'future','pending')", (sid, store.clock()))
        store.recover()
        assert store.db.execute("SELECT sha256 FROM audio_segments").fetchone()[0] == original_hash
        assert (path.read_bytes() if path.exists() else None) == damaged
        assert store.db.execute("SELECT state FROM processing_jobs").fetchone()[0] == "blocked"
        assert store.db.execute("SELECT recovery_status FROM recording_sessions").fetchone()[0] == "needs_attention"
        repair = store.segments(sid)[0]
        assert repair["frame_count"] == 13 and repair["recovered"]
        if damage == "repair_damaged":
            repaired_path = artifact_path(root, repair["relative_path"])
            repaired_path.write_bytes(b"broken repair")
            findings = store.recover()
            assert any(f["code"] == "recovery_artifact_invalid" for f in findings)
            assert repaired_path.read_bytes() == b"broken repair"
            assert store.segments(sid) == []
        before = snapshot(store)
        store.recover()
        assert snapshot(store) == before


def test_store_lock_excludes_writer_or_recovery_in_another_owner(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        with pytest.raises(OSError):
            SegmentStore(repository=checkout, root=root)
        with pytest.raises(RuntimeError):
            store.recover()
        with pytest.raises(RuntimeError):
            store.writer(sid, writer.interval_id)


@pytest.mark.parametrize("boundary", ["pcm.before_flush", "checkpoint.before_commit", "checkpoint.committed"])
def test_explicit_checkpoint_failure_closes_writer(external, checkout, boundary):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        feed(writer, 3)
        store.io = FaultIO(boundary)
        with pytest.raises(PersistenceError):
            writer.checkpoint()
        assert writer.closed and writer.fd is None
        with pytest.raises(PersistenceError):
            feed(writer, 1)
        with pytest.raises(PersistenceError):
            writer.finish()
        store.io = DurableIO()
        store.recover()
        assert sum(r["frame_count"] for r in store.segments(sid)) == 3


@pytest.mark.parametrize("label", ["publication_intent", "finalized", "session_finish"])
def test_sqlite_commit_rejection_preserves_files_and_rolls_back(external, checkout, label):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)

        class RejectCommitIO(DurableIO):
            denied = False

            def hook(self, boundary):
                if boundary == label + ".before_commit":
                    store.db.set_authorizer(self.authorize)

            def authorize(self, action, arg1, arg2, database_name, trigger):
                if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not self.denied:
                    self.denied = True
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        io = RejectCommitIO()
        store.io = io
        with pytest.raises(PersistenceError):
            writer.finish()
        assert io.denied
        store.db.set_authorizer(None)
        assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "interrupted"
        originals = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        store.io = DurableIO()
        store.recover()
        assert sum(r["frame_count"] for r in store.segments(sid)) == 13
        for name, value in originals.items():
            assert (root / "audio" / name).read_bytes() == value


def test_finalized_sql_trigger_failure_leaves_adoptable_publication(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        feed(writer, 13)
        store.db.execute("CREATE TRIGGER fail_finalized BEFORE UPDATE OF state ON audio_segments WHEN NEW.state='finalized' BEGIN SELECT RAISE(ABORT,'synthetic metadata failure'); END")
        with pytest.raises(PersistenceError):
            writer.finish()
        assert store.db.execute("SELECT state FROM audio_segments").fetchone()[0] == "writing"
        store.db.execute("DROP TRIGGER fail_finalized")
        findings = store.recover()
        assert any(f["code"] == "adopted_publication" for f in findings)
        assert not store.db.execute("SELECT 1 FROM segment_recoveries").fetchone()
        assert store.segments(sid)[0]["frame_count"] == 13


def test_audio_directory_junction_into_repository_rejected(external, checkout):
    root = external / "data"
    root.mkdir()
    alias = root / "audio"
    if os.name == "nt":
        result = subprocess.run(["cmd", "/c", "mklink", "/J", str(alias), str(checkout)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    else:
        alias.symlink_to(checkout, target_is_directory=True)
    with pytest.raises(ValueError, match="Redirected"):
        SegmentStore(repository=checkout, root=root)


def test_v1_upgrade_preserves_existing_metadata_and_failed_v2_rolls_back(external, monkeypatch):
    from hashlib import sha256
    from test_database import seed

    path = external / "legacy.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    first = database._migration_sql(1)
    digest = sha256(first.encode()).hexdigest()
    db.executescript("BEGIN;" + first + f"INSERT INTO schema_migrations VALUES(1,'{digest}'); PRAGMA user_version=1; COMMIT;")
    seed(db)
    original = database._migration_sql
    monkeypatch.setattr(database, "_migration_sql", lambda version=1: original(version) + ("\nINVALID SQL;" if version == 2 else ""))
    with pytest.raises(sqlite3.OperationalError):
        database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 1
    assert db.execute("SELECT name FROM sqlite_master WHERE name='segment_storage'").fetchall() == []
    monkeypatch.setattr(database, "_migration_sql", original)
    database.migrate(db)
    assert db.execute("PRAGMA user_version").fetchone()[0] == 2
    assert db.execute("SELECT count(*) FROM recording_sessions").fetchone()[0] == 1
    db.close()


CRASH_SCRIPT = """
import os, sys
from pathlib import Path
from personal_log.storage.segments import SegmentStore
from personal_log.storage.files import DurableIO
from personal_log.recording.fake import FakeMicrophone, AudioFormat
class Kill(DurableIO):
    def hook(self, boundary):
        if boundary == sys.argv[3]:
            os._exit(77)
with SegmentStore(repository=Path(sys.argv[1]), root=Path(sys.argv[2]), io=Kill()) as store:
    sid, interval = store.begin_session(audio_format=AudioFormat(sample_rate=10))
    writer = store.writer(sid, interval)
    fake = FakeMicrophone(AudioFormat(sample_rate=10))
    fake.start()
    writer.append(fake.read(10))
    writer.append(fake.read(3))
    writer.finish()
"""


@pytest.mark.parametrize("boundary", [
    "segment_intent.committed", "stage.created", "accepted.committed", "pcm.written",
    "pcm.flushed", "checkpoint.committed", "wav.created", "wav.flushed",
    "publication_intent.committed", "publication.after", "finalized.before_commit",
    "finalized.committed", "session_finish.before_commit", "session_finish.committed",
])
def test_process_termination_at_critical_boundaries(external, checkout, boundary):
    root = external / "data"
    result = subprocess.run([sys.executable, "-c", CRASH_SCRIPT, str(checkout), str(root), boundary],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 77, result.stderr
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        original_files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        findings = store.recover()
        for name, value in original_files.items():
            assert (root / "audio" / name).read_bytes() == value
        before = snapshot(store)
        artifacts = sorted(p.name for p in (root / "audio").iterdir())
        assert store.recover() == findings
        assert snapshot(store) == before
        assert sorted(p.name for p in (root / "audio").iterdir()) == artifacts
        sid = store.db.execute("SELECT session_id FROM recording_sessions").fetchone()[0]
        refs = store.segments(sid)
        expected_frames = {
            "segment_intent.committed": 0, "stage.created": 0, "accepted.committed": 0,
            "pcm.written": 10, "pcm.flushed": 10, "checkpoint.committed": 10,
        }.get(boundary, 13)
        assert sum(r["frame_count"] for r in refs) == expected_frames
        if boundary != "session_finish.committed":
            assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "recovery_required"
        if refs:
            recovered = read_pcm(store, refs)
            fake = FakeMicrophone(AudioFormat(sample_rate=RATE))
            fake.start()
            expected = fake.read(min(expected_frames, RATE)).pcm
            if expected_frames > RATE:
                expected += fake.read(expected_frames - RATE).pcm
            assert recovered == expected


RECOVERY_CRASH_SCRIPT = """
import os, sys
from pathlib import Path
from personal_log.storage.segments import SegmentStore
from personal_log.storage.files import DurableIO
class Kill(DurableIO):
    def hook(self, boundary):
        if boundary == sys.argv[3]:
            os._exit(78)
with SegmentStore(repository=Path(sys.argv[1]), root=Path(sys.argv[2]), io=Kill()) as store:
    store.recover()
"""


@pytest.mark.parametrize("boundary", [
    "recovery_session.before_commit", "recovery_session.committed", "wav.created",
    "wav.written", "wav.flushed", "recovery_publication.before", "recovery_publication.after",
    "recovery_metadata.before_commit", "recovery_metadata.committed",
    "recovery_status.before_commit", "recovery_status.committed",
])
def test_recovery_itself_survives_process_termination(external, checkout, boundary):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        stage = artifact_path(root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        original = stage.read_bytes()
    result = subprocess.run([sys.executable, "-c", RECOVERY_CRASH_SCRIPT, str(checkout), str(root), boundary],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 78, result.stderr
    with SegmentStore(repository=checkout, root=root) as store:
        interrupted_files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        findings = store.recover()
        assert stage.read_bytes() == original
        refs = store.segments(sid)
        assert len(refs) == 1 and refs[0]["frame_count"] == 13
        assert read_pcm(store, refs) == original
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 1
        for name, value in interrupted_files.items():
            assert (root / "audio" / name).read_bytes() == value
        before = snapshot(store)
        names = sorted(p.name for p in (root / "audio").iterdir())
        assert store.recover() == findings
        assert snapshot(store) == before
        assert sorted(p.name for p in (root / "audio").iterdir()) == names


def test_repeated_crashes_before_recovery_commit_do_not_duplicate_artifacts(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
    files = None
    for _ in range(3):
        result = subprocess.run([sys.executable, "-c", RECOVERY_CRASH_SCRIPT, str(checkout), str(root), "recovery_metadata.before_commit"],
                                capture_output=True, text=True, timeout=20)
        assert result.returncode == 78, result.stderr
        current = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        if files is not None:
            assert current == files
        files = current
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        assert len(store.segments(sid)) == 1
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 1
        assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == files


@pytest.mark.parametrize("boundary", ["recovery_adopt.before_commit", "recovery_adopt.committed"])
def test_interrupted_adoption_of_published_orphan(external, checkout, boundary):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root, io=FaultIO("finalized.before_commit")) as store:
        sid, writer = start(store)
        feed(writer, 13)
        with pytest.raises(PersistenceError):
            writer.finish()
    result = subprocess.run([sys.executable, "-c", RECOVERY_CRASH_SCRIPT, str(checkout), str(root), boundary],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 78, result.stderr
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        assert len(store.segments(sid)) == 1
        assert not store.segments(sid)[0]["recovered"]
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 0


def test_recovery_metadata_commit_rejection_preserves_published_repair(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        writer.abandon()

        class RejectRecoveryCommitIO(DurableIO):
            denied = False

            def hook(self, boundary):
                if boundary == "recovery_metadata.before_commit":
                    store.db.set_authorizer(self.authorize)

            def authorize(self, action, arg1, *unused):
                if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not self.denied:
                    self.denied = True
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        store.io = RejectRecoveryCommitIO()
        with pytest.raises(sqlite3.DatabaseError):
            store.recover()
        assert store.io.denied
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 0
        files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        store.db.set_authorizer(None)
        store.io = DurableIO()
        store.recover()
        assert len(store.segments(sid)) == 1
        assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == files


def test_recovery_conflict_preserves_both_source_and_foreign_candidate(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        segment_id = store.db.execute("SELECT segment_id FROM audio_segments").fetchone()[0]
        writer.abandon()
        candidate = root / "audio" / f"{segment_id}.recovered.wav"
        candidate.write_bytes(b"preserve conflicting candidate")
        findings = store.recover()
        assert any(f["code"] == "recovery_publication_conflict" for f in findings)
        assert not any(f["code"] == "orphan_artifact" and f["source_path"] == candidate.relative_to(root).as_posix() for f in findings)
        assert store.segments(sid) == []
        assert candidate.read_bytes() == b"preserve conflicting candidate"
        before = snapshot(store)
        files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        assert store.recover() == findings
        assert snapshot(store) == before
        assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == files
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 0


def test_missing_stage_unknown_orphans_and_ordering_gaps_are_reported(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        writer.abandon()
        stage = artifact_path(root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        stage.unlink()  # Synthetic missing artifact outside repository.
        store.db.execute("UPDATE audio_segments SET sequence=1,start_frame=5")
        orphan = root / "audio" / "unknown.pcm.part"
        orphan.write_bytes(b"unknown preserved audio")
        findings = store.recover()
        codes = {f["code"] for f in findings}
        assert codes >= {"missing_frames", "no_recoverable_frames", "sequence_gap", "frame_gap", "orphan_artifact"}
        assert orphan.read_bytes() == b"unknown preserved audio"
        assert store.segments(sid) == []


@pytest.mark.parametrize("flush_number", [2, 3, 4])
def test_syscall_flush_failure_during_finalization_or_publication(external, checkout, flush_number):
    class FailNthFlushIO(DurableIO):
        calls = 0

        def flush(self, fd):
            self.calls += 1
            if self.calls == flush_number:
                raise OSError("Synthetic fsync syscall failure")
            super().flush(fd)

    root = external / "data"
    with SegmentStore(repository=checkout, root=root, io=FailNthFlushIO()) as store:
        sid, writer = start(store)
        feed(writer, 13)
        with pytest.raises(PersistenceError):
            writer.finish()
        assert store.db.execute("SELECT count(*) FROM audio_segments WHERE state='finalized'").fetchone()[0] == 0
        files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        store.io = DurableIO()
        store.recover()
        assert store.segments(sid)[0]["frame_count"] == 13
        for name, value in files.items():
            assert (root / "audio" / name).read_bytes() == value


def test_partial_wav_write_failure_preserves_builder_and_pcm(external, checkout):
    class FailWavWriteIO(DurableIO):
        building = False
        calls = 0

        def hook(self, boundary):
            if boundary == "wav.created":
                self.building = True

        def write(self, fd, data):
            if self.building:
                self.calls += 1
                if self.calls == 1:
                    return super().write(fd, data[:7])
                raise OSError("Synthetic partial WAV write")
            return super().write(fd, data)

    root = external / "data"
    with SegmentStore(repository=checkout, root=root, io=FailWavWriteIO()) as store:
        sid, writer = start(store)
        feed(writer, 13)
        with pytest.raises(PersistenceError):
            writer.finish()
        builds = list((root / "audio").glob("*.build"))
        assert len(builds) == 1 and builds[0].stat().st_size == 7
        preserved = builds[0].read_bytes()
        store.io = DurableIO()
        store.recover()
        assert builds[0].read_bytes() == preserved
        assert store.segments(sid)[0]["frame_count"] == 13


def test_changed_staging_bytes_cannot_replace_committed_audio(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        ref = writer.finish()[0]
        final = artifact_path(root, ref["relative_path"])
        final.write_bytes(b"damaged publication")
        stage = artifact_path(root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        stage.write_bytes(b"\x01\x02" * 13)
        findings = store.recover()
        assert any(f["code"] == "staging_hash_mismatch" for f in findings)
        assert store.segments(sid) == []
        assert final.read_bytes() == b"damaged publication"
        assert stage.read_bytes() == b"\x01\x02" * 13


def test_valid_recovered_segment_is_usable_and_lookup_is_read_only(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        feed(writer, 13)
        writer.abandon()
        assert store.segments(sid) == []
        store.recover()
        before = snapshot(store)
        refs = store.segments(sid)
        assert len(refs) == 1 and refs[0]["recovered"]
        assert refs[0]["frame_count"] == 13
        stage = artifact_path(store.root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        assert read_pcm(store, refs) == stage.read_bytes()
        assert snapshot(store) == before
        store.recover()
        assert snapshot(store) == before
        assert store.segments(sid) == refs


@pytest.mark.parametrize("damage", ["truncated", "mutated", "deleted", "directory"])
def test_effective_repair_rejects_later_damage_before_and_after_recovery(external, checkout, damage):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        feed(writer, 13)
        original_ref = writer.finish()[0]
        original = artifact_path(store.root, original_ref["relative_path"])
        original.write_bytes(b"synthetic damaged original")
        store.recover()
        ref = store.segments(sid)[0]
        repaired = artifact_path(store.root, ref["relative_path"])
        assert ref["recovered"]
        immutable_metadata = tuple(store.db.execute("SELECT * FROM segment_recoveries").fetchone())
        stage = artifact_path(store.root, store.db.execute("SELECT staging_path FROM segment_storage").fetchone()[0])
        preserved_pcm = stage.read_bytes()
        if damage == "truncated":
            repaired.write_bytes(repaired.read_bytes()[:47])
        elif damage == "mutated":
            changed = bytearray(repaired.read_bytes())
            changed[-1] ^= 1
            repaired.write_bytes(changed)
        else:
            repaired.unlink()  # Synthetic damage to an external disposable artifact.
            if damage == "directory":
                repaired.mkdir()
        preserved_repair = repaired.read_bytes() if repaired.is_file() else None

        # Lookup must not rely on a previous recovery pass to know availability.
        before_lookup = snapshot(store)
        assert store.segments(sid) == []
        assert snapshot(store) == before_lookup
        findings = store.recover()
        assert any(f["code"] == "recovery_artifact_invalid" and f["segment_id"] == ref["segment_id"] for f in findings)
        assert store.segments(sid) == []
        before = snapshot(store)
        for _ in range(2):
            assert store.recover() == findings
            assert snapshot(store) == before
            assert store.segments(sid) == []
            assert original.read_bytes() == b"synthetic damaged original"
            assert stage.read_bytes() == preserved_pcm
            assert tuple(store.db.execute("SELECT * FROM segment_recoveries").fetchone()) == immutable_metadata
            if damage == "deleted":
                assert not repaired.exists()
            elif damage == "directory":
                assert repaired.is_dir()
            else:
                assert repaired.read_bytes() == preserved_repair


@pytest.mark.parametrize("stale_orphan", [False, True])
def test_pending_repair_adoption_has_no_stale_orphan_warning(external, checkout, stale_orphan):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root) as store:
        sid, writer = start(store)
        feed(writer, 13)
        writer.abandon()
        segment_id = store.db.execute("SELECT segment_id FROM audio_segments").fetchone()[0]
    result = subprocess.run([sys.executable, "-c", RECOVERY_CRASH_SCRIPT, str(checkout), str(root), "recovery_publication.after"],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 78, result.stderr
    relative = f"audio/{segment_id}.recovered.wav"
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 0
        pending = artifact_path(root, relative)
        preserved = pending.read_bytes()
        assert inspect_wav(pending, RATE).frames == 13
        if stale_orphan:
            # Simulate historical misclassification; preserve that evidence while
            # the immutable adoption record resolves its active warning.
            store.db.execute("INSERT INTO recovery_findings (finding_id,code,severity,source_path,created_at) VALUES ('00000000-0000-0000-0000-000000000099','orphan_artifact','uncertain',?,?)", (relative, store.clock()))
        files = {p.name: p.read_bytes() for p in (root / "audio").iterdir()}
        findings = store.recover()
        assert not any(f["code"] == "orphan_artifact" and f["source_path"] == relative for f in findings)
        assert pending.read_bytes() == preserved
        assert store.db.execute("SELECT count(*) FROM segment_recoveries WHERE segment_id=?", (segment_id,)).fetchone()[0] == 1
        assert store.segments(sid)[0]["relative_path"] == relative
        assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == files
        historical = store.db.execute("SELECT count(*) FROM recovery_findings WHERE code='orphan_artifact' AND source_path=?", (relative,)).fetchone()[0]
        assert historical == int(stale_orphan)
        before = snapshot(store)
        for _ in range(2):
            assert store.recover() == findings
            assert snapshot(store) == before
            assert {p.name: p.read_bytes() for p in (root / "audio").iterdir()} == files


def test_foreign_repair_name_beside_intact_original_remains_an_orphan(external, checkout):
    with SegmentStore(repository=checkout, root=external / "data") as store:
        sid, writer = start(store)
        feed(writer, 13)
        original_ref = writer.finish()[0]
        relative = f"audio/{original_ref['segment_id']}.recovered.wav"
        foreign = artifact_path(store.root, relative)
        foreign.write_bytes(b"unrelated preserved file")
        findings = store.recover()
        assert any(f["code"] == "orphan_artifact" and f["source_path"] == relative for f in findings)
        assert store.segments(sid) == [original_ref]
        assert foreign.read_bytes() == b"unrelated preserved file"
        assert store.db.execute("SELECT count(*) FROM segment_recoveries").fetchone()[0] == 0
        before = snapshot(store)
        assert store.recover() == findings
        assert snapshot(store) == before
