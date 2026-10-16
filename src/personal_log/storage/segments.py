"""Synchronous storage worker for synthetic PCM; never an audio callback.

One SegmentStore owns the external root and its SQLite connection exclusively.
The eventual recorder process can feed this worker without importing UI or AI.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
from uuid import UUID, uuid4

from personal_log.recording.fake import AudioBlock, AudioFormat
from .database import open_database
from .files import DurableIO, StoreLock, WavInfo, artifact_path, inspect_wav, valid_wav, wav_header
from .paths import data_file, prepare_data_root


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class PersistenceError(RuntimeError):
    """No successful save acknowledgment was issued; recovery may be required."""


class SegmentStore:
    def __init__(self, *, repository: Path, root: Path | None = None, io: DurableIO | None = None, clock=utc_now):
        self.root = prepare_data_root(repository=repository, root=root)
        self.io, self.clock = io or DurableIO(), clock
        self._writers = []
        self._lock = StoreLock(data_file(self.root, "recording.lock"))
        self.db = None
        try:
            self.db = open_database(repository=repository, root=self.root)
            self.db.row_factory = sqlite3.Row
            directory = artifact_path(self.root, "audio")
            directory.mkdir(exist_ok=True)
            artifact_path(self.root, "audio")
            self.io.sync_directory(self.root)
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        for writer in self._writers:
            writer.abandon()
        if self.db is not None:
            self.db.close()
            self.db = None
        self._lock.close()

    @contextmanager
    def transaction(self, label: str):
        if self.db.in_transaction:
            raise RuntimeError("Nested storage transactions are not supported")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.io.hook(label + ".before_commit")
            self.db.commit()
            self.io.hook(label + ".committed")
        except BaseException:
            if self.db.in_transaction:
                self.db.rollback()
            raise

    def state(self, session_id: str, state: str, code: str = "state_changed"):
        """Must be called inside a transaction, including its lifecycle event."""
        if not self.db.in_transaction:
            raise RuntimeError("State changes require a transaction")
        self.db.execute("UPDATE recording_sessions SET state=? WHERE session_id=?", (state, session_id))
        sequence = self.db.execute("SELECT coalesce(max(sequence),-1)+1 FROM lifecycle_events WHERE session_id=?", (session_id,)).fetchone()[0]
        self.db.execute("INSERT INTO lifecycle_events (event_id,session_id,sequence,created_at,state,code) VALUES (?,?,?,?,?,?)",
                        (str(uuid4()), session_id, sequence, self.clock(), state, code))

    def begin_session(self, *, audio_format: AudioFormat | None = None, entry_id: str | None = None) -> tuple[str, str]:
        """Create disposable metadata; supplying an entry ID adds a supplement."""
        fmt = audio_format or AudioFormat()
        session_id, interval_id = str(uuid4()), str(uuid4())
        with self.transaction("session_start"):
            if entry_id is None:
                entry_id = str(uuid4())
                self.db.execute("INSERT INTO journal_entries (entry_id,created_at,display_timezone) VALUES (?,?,'UTC')", (entry_id, self.clock()))
            self.db.execute("INSERT INTO recording_sessions (session_id,entry_id,created_at,state) VALUES (?,?,?,'starting')",
                            (session_id, entry_id, self.clock()))
            self.db.execute("INSERT INTO capture_intervals (interval_id,session_id,sequence,started_at,device_id,sample_rate,channels,sample_width) VALUES (?,?,0,?,'synthetic',?,1,2)",
                            (interval_id, session_id, self.clock(), fmt.sample_rate))
            self.state(session_id, "recording")
        return session_id, interval_id

    def writer(self, session_id: str, interval_id: str):
        if any(not w.closed for w in self._writers):
            raise RuntimeError("Only one writer may own capture")
        writer = SegmentWriter(self, session_id, interval_id)
        self._writers.append(writer)
        return writer

    def segments(self, session_id: str) -> list[dict]:
        """Ordered, currently validated references; never modifies artifacts.

        Recovery reconciles durable metadata; this read also rejects subsequent
        external damage before a recovery run has recorded an invalid finding.
        """
        candidates = self.db.execute(
            "SELECT s.segment_id,s.interval_id,s.sequence,s.start_frame,i.sample_rate,"
            "coalesce(r.relative_path,s.relative_path) AS relative_path,"
            "coalesce(r.frame_count,s.frame_count) AS frame_count,"
            "coalesce(r.byte_length,s.byte_length) AS byte_length,"
            "coalesce(r.sha256,s.sha256) AS sha256,r.segment_id IS NOT NULL AS recovered "
            "FROM audio_segments s JOIN capture_intervals i ON i.interval_id=s.interval_id "
            "LEFT JOIN segment_recoveries r ON r.segment_id=s.segment_id "
            "WHERE s.session_id=? AND (r.segment_id IS NOT NULL OR s.state IN ('finalized','recovered')) "
            "AND NOT EXISTS (SELECT 1 FROM recovery_findings f WHERE f.segment_id=s.segment_id "
            "AND f.code='recovery_artifact_invalid') ORDER BY s.sequence", (session_id,))
        references = []
        for candidate in candidates:
            item = dict(candidate)
            rate = item.pop("sample_rate")
            expected = WavInfo(item["frame_count"], item["byte_length"], item["sha256"])
            if valid_wav(self.root, item["relative_path"], rate, expected):
                references.append(item)
        return references

    def recover(self):
        from .recovery import recover
        if any(not w.closed for w in self._writers):
            raise RuntimeError("Recovery cannot run alongside an active writer")
        return recover(self)

    def build_wav(self, source: Path, target: Path, frames: int, sample_rate: int) -> tuple[Path, WavInfo]:
        """Build a new artifact, retaining source and failed temporary artifacts."""
        temporary = artifact_path(self.root, target.relative_to(self.root).as_posix() + f".{uuid4()}.build")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        try:
            self.io.hook("wav.created")
            expected_hash = sha256()
            header = wav_header(frames, sample_rate)
            self.io.write_all(fd, header)
            expected_hash.update(header)
            remaining = frames * 2
            with source.open("rb") as pcm:
                while remaining:
                    chunk = pcm.read(min(65536, remaining))
                    if not chunk:
                        raise PersistenceError("PCM source truncated during finalization")
                    self.io.write_all(fd, chunk)
                    expected_hash.update(chunk)
                    remaining -= len(chunk)
            self.io.hook("wav.written")
            self.io.flush(fd)
            self.io.hook("wav.flushed")
        finally:
            os.close(fd)
        info = inspect_wav(temporary, sample_rate)
        if info != WavInfo(frames, 44 + frames * 2, expected_hash.hexdigest()):
            raise PersistenceError("WAV validation failed")
        return temporary, info


class SegmentWriter:
    def __init__(self, store: SegmentStore, session_id: str, interval_id: str):
        # Validate before using IDs as path components, even for direct callers.
        if str(UUID(session_id)) != session_id or str(UUID(interval_id)) != interval_id:
            raise ValueError("Noncanonical ID")
        interval = store.db.execute("SELECT i.*,s.state FROM capture_intervals i JOIN recording_sessions s USING(session_id) WHERE i.interval_id=? AND i.session_id=?", (interval_id, session_id)).fetchone()
        if interval is None or interval["state"] != "recording" or interval["ended_at"] is not None:
            raise ValueError("Writer requires an open recording interval")
        if store.db.execute("SELECT 1 FROM audio_segments WHERE interval_id=?", (interval_id,)).fetchone():
            raise ValueError("Existing intervals require recovery, not a new writer")
        self.store, self.session_id, self.interval_id = store, session_id, interval_id
        self.sample_rate = interval["sample_rate"]
        self.segment_frames = self.sample_rate * 20
        self.position = 0
        self.sequence = store.db.execute("SELECT coalesce(max(sequence),-1)+1 FROM audio_segments WHERE session_id=?", (session_id,)).fetchone()[0]
        self.fd = None
        self.segment_id = None
        self.frames = 0
        self.durable = 0
        self.closed = False

    def abandon(self):
        """Close handles without finalizing, as after a crash; never claim Saved."""
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.closed = True

    def _fail(self):
        self.abandon()
        try:
            with self.store.transaction("recording_failure"):
                state = self.store.db.execute("SELECT state FROM recording_sessions WHERE session_id=?", (self.session_id,)).fetchone()[0]
                if state not in {"finished", "interrupted", "recovery_required"}:
                    self.store.db.execute("UPDATE recording_sessions SET interruption_code='storage_failed' WHERE session_id=?", (self.session_id,))
                    self.store.state(self.session_id, "interrupted", "storage_failed")
        except Exception:
            pass  # Original persistence error wins; abandonment is recoverable.

    def _start_segment(self):
        self.segment_id = str(uuid4())
        self.relative = f"audio/{self.segment_id}.wav"
        self.staging = f"audio/{self.segment_id}.pcm.part"
        with self.store.transaction("segment_intent"):
            self.store.db.execute("INSERT INTO audio_segments (segment_id,session_id,interval_id,sequence,created_at,start_frame,relative_path,state) VALUES (?,?,?,?,?,?,?,'writing')",
                                  (self.segment_id, self.session_id, self.interval_id, self.sequence, self.store.clock(), self.position, self.relative))
            self.store.db.execute("INSERT INTO segment_storage (segment_id,staging_path) VALUES (?,?)", (self.segment_id, self.staging))
        self.store.io.hook("stage.before_create")
        self.fd = os.open(artifact_path(self.store.root, self.staging), os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        self.store.io.sync_directory(artifact_path(self.store.root, "audio"))
        self.store.io.hook("stage.created")
        self.frames = self.durable = 0

    def checkpoint(self):
        if self.closed:
            raise PersistenceError("Writer is closed")
        try:
            self._checkpoint()
        except Exception as error:
            self._fail()
            raise PersistenceError("Audio checkpoint failed; recovery required") from error

    def _checkpoint(self):
        if self.fd is not None:
            self.store.io.hook("pcm.before_flush")
            self.store.io.flush(self.fd)
            self.store.io.hook("pcm.flushed")
            with self.store.transaction("checkpoint"):
                self.store.db.execute("UPDATE segment_storage SET durable_frames=? WHERE segment_id=?", (self.frames, self.segment_id))
            self.durable = self.frames

    def append(self, block: AudioBlock):
        if self.closed:
            raise PersistenceError("Writer is closed")
        try:
            if (type(block.start_frame) is not int or type(block.frames) is not int or
                    block.frames <= 0 or block.start_frame != self.position or len(block.pcm) != block.frames * 2):
                raise ValueError("PCM block does not preserve frame continuity")
            offset = 0
            while offset < block.frames:
                if self.fd is None:
                    self._start_segment()
                take = min(block.frames - offset, self.segment_frames - self.frames,
                           self.sample_rate - (self.frames - self.durable))
                with self.store.transaction("accepted"):
                    self.store.db.execute("UPDATE segment_storage SET accepted_frames=? WHERE segment_id=?", (self.frames + take, self.segment_id))
                self.store.io.hook("pcm.before_write")
                self.store.io.write_all(self.fd, block.pcm[offset * 2:(offset + take) * 2])
                self.store.io.hook("pcm.written")
                self.frames += take
                self.position += take
                offset += take
                if self.frames - self.durable >= self.sample_rate:
                    self.checkpoint()
                if self.frames == self.segment_frames:
                    self._finalize()
        except Exception as error:
            self._fail()
            raise PersistenceError("Audio persistence failed; recovery required") from error

    def _finalize(self):
        self.checkpoint()
        os.close(self.fd)
        self.fd = None
        target = artifact_path(self.store.root, self.relative)
        temporary, info = self.store.build_wav(artifact_path(self.store.root, self.staging), target, self.frames, self.sample_rate)
        with self.store.transaction("publication_intent"):
            self.store.db.execute("UPDATE segment_storage SET publication_sha256=?,publication_bytes=?,publication_frames=? WHERE segment_id=?",
                                  (info.sha256, info.byte_length, info.frames, self.segment_id))
        self.store.io.hook("publication.before")
        self.store.io.publish(temporary, target)
        self.store.io.hook("publication.after")
        with self.store.transaction("finalized"):
            self.store.db.execute("UPDATE audio_segments SET state='finalized',frame_count=?,byte_length=?,sha256=? WHERE segment_id=?",
                                  (info.frames, info.byte_length, info.sha256, self.segment_id))
        self.sequence += 1
        self.segment_id = None

    def finish(self) -> list[dict]:
        if self.closed:
            raise PersistenceError("Writer is closed")
        try:
            if self.fd is not None:
                self._finalize()
            with self.store.transaction("session_finish"):
                self.store.state(self.session_id, "finalizing")
                self.store.db.execute("UPDATE capture_intervals SET ended_at=?,end_reason='finish' WHERE interval_id=?", (self.store.clock(), self.interval_id))
                self.store.state(self.session_id, "finished")
                self.store.db.execute("UPDATE recording_sessions SET recovery_status='clean' WHERE session_id=?", (self.session_id,))
            self.closed = True
            return self.store.segments(self.session_id)
        except Exception as error:
            self._fail()
            raise PersistenceError("Recording was not acknowledged as saved") from error
