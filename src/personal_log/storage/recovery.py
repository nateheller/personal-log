"""Deterministic reconciliation under SegmentStore's exclusive OS lock.

Never starts capture, overwrites artifacts, deletes source PCM, or blesses a
changed hash. An original segment keeps its identity/order; an immutable repair
record supplies an alternative artifact without erasing original metadata.
"""

from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from .files import WavInfo, artifact_path, valid_wav, wav_header
from .segments import PersistenceError


def _finding(store, session_id, segment_id, code, severity, path="", missing=0, partial=0):
    identity = str(uuid5(NAMESPACE_URL, f"personal-log/recovery/v1/{session_id}/{segment_id}/{code}/{path}"))
    store.db.execute("INSERT INTO recovery_findings (finding_id,session_id,segment_id,code,severity,source_path,missing_frames,partial_bytes,created_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(finding_id) DO NOTHING",
                     (identity, session_id, segment_id, code, severity, path, missing, partial, store.clock()))


def _expected_pcm_wav(path, frames, rate):
    digest = sha256(wav_header(frames, rate))
    remaining = frames * 2
    with path.open("rb") as source:
        while remaining:
            chunk = source.read(min(65536, remaining))
            if not chunk:
                raise PersistenceError("Recovery source changed while reading")
            digest.update(chunk)
            remaining -= len(chunk)
    return WavInfo(frames, 44 + frames * 2, digest.hexdigest())


def _repair(store, row):
    sid, segment_id, rate = row["session_id"], row["segment_id"], row["sample_rate"]
    repair = store.db.execute("SELECT * FROM segment_recoveries WHERE segment_id=?", (segment_id,)).fetchone()
    if repair:
        expected = WavInfo(repair["frame_count"], repair["byte_length"], repair["sha256"])
        if not valid_wav(store.root, repair["relative_path"], rate, expected):
            with store.transaction("recovery_issue"):
                _finding(store, sid, segment_id, "recovery_artifact_invalid", "error", repair["relative_path"])
        return  # Never replace an earlier repair, even if it is damaged later.

    expected = WavInfo(row["frame_count"], row["byte_length"], row["sha256"])
    if row["state"] in {"finalized", "recovered"} and valid_wav(store.root, row["relative_path"], rate, expected):
        return

    # A published orphan is accepted only against a durable pre-publication hash.
    if row["state"] == "writing" and row["publication_sha256"] is not None:
        intended = WavInfo(row["publication_frames"], row["publication_bytes"], row["publication_sha256"])
        if valid_wav(store.root, row["relative_path"], rate, intended):
            store.io.sync_published(artifact_path(store.root, row["relative_path"]))
            with store.transaction("recovery_adopt"):
                store.db.execute("UPDATE audio_segments SET state='finalized',frame_count=?,byte_length=?,sha256=? WHERE segment_id=?",
                                 (intended.frames, intended.byte_length, intended.sha256, segment_id))
                _finding(store, sid, segment_id, "adopted_publication", "info", row["relative_path"])
            return

    stage = artifact_path(store.root, row["staging_path"]) if row["staging_path"] else None
    size = stage.stat().st_size if stage is not None and stage.exists() else 0
    accepted = row["accepted_frames"] if row["accepted_frames"] is not None else row["frame_count"]
    frames = min(size // 2, accepted)
    partial, missing = size % 2, max(0, accepted - frames)
    original = artifact_path(store.root, row["relative_path"])
    with store.transaction("recovery_issue"):
        if original.exists():
            _finding(store, sid, segment_id, "invalid_publication", "error", row["relative_path"])
        elif row["state"] in {"finalized", "recovered"}:
            _finding(store, sid, segment_id, "missing_publication", "error", row["relative_path"])
        if missing:
            _finding(store, sid, segment_id, "missing_frames", "gap", row["staging_path"] or "", missing=missing)
        if partial:
            _finding(store, sid, segment_id, "partial_frame", "gap", row["staging_path"], partial=partial)
        if size > accepted * 2:
            _finding(store, sid, segment_id, "staging_excess", "uncertain", row["staging_path"], partial=size - accepted * 2)
        if not frames:
            _finding(store, sid, segment_id, "no_recoverable_frames", "error", row["staging_path"] or row["relative_path"])
        store.db.execute("UPDATE audio_segments SET state='unresolved' WHERE segment_id=?", (segment_id,))
    if not frames:
        return

    relative = f"audio/{segment_id}.recovered.wav"
    target = artifact_path(store.root, relative)
    expected = _expected_pcm_wav(stage, frames, rate)
    # A full-length staging copy can be checked against the original publication
    # commitment. Never turn a changed raw source into an apparently valid repair.
    committed_hash = row["sha256"] or row["publication_sha256"]
    if frames == accepted and committed_hash is not None and expected.sha256 != committed_hash:
        with store.transaction("recovery_issue"):
            _finding(store, sid, segment_id, "staging_hash_mismatch", "error", row["staging_path"])
        return
    if target.exists():
        if not valid_wav(store.root, relative, rate, expected):
            with store.transaction("recovery_issue"):
                _finding(store, sid, segment_id, "recovery_publication_conflict", "error", relative)
            return
        store.io.sync_published(target)
    else:
        temporary, info = store.build_wav(stage, target, frames, rate)
        if info != expected:
            raise PersistenceError("Recovery source changed during WAV creation")
        store.io.hook("recovery_publication.before")
        store.io.publish(temporary, target)
        store.io.hook("recovery_publication.after")
    with store.transaction("recovery_metadata"):
        store.db.execute("INSERT INTO segment_recoveries (segment_id,relative_path,source_path,frame_count,byte_length,sha256,missing_frames,partial_bytes,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                         (segment_id, relative, row["staging_path"], frames, expected.byte_length, expected.sha256, missing, partial, store.clock()))
        _finding(store, sid, segment_id, "recovered_pcm", "info", row["staging_path"])


def recover(store) -> list[dict]:
    """Return persistent findings, including uncertain tails; never Saved status."""
    sessions = store.db.execute("SELECT session_id,state FROM recording_sessions ORDER BY session_id").fetchall()
    for session in sessions:
        sid = session["session_id"]
        if session["state"] != "finished":
            with store.transaction("recovery_session"):
                if session["state"] not in {"interrupted", "recovery_required"}:
                    store.db.execute("UPDATE recording_sessions SET interruption_code='recorder_crash' WHERE session_id=?", (sid,))
                    store.state(sid, "interrupted", "recorder_crash")
                if session["state"] != "recovery_required":
                    store.state(sid, "recovery_required", "recovery_required")
                store.db.execute("UPDATE capture_intervals SET ended_at=?,end_reason='recorder_crash' WHERE session_id=? AND ended_at IS NULL", (store.clock(), sid))
                _finding(store, sid, None, "capture_tail_unknown", "uncertain")

    rows = store.db.execute(
        "SELECT s.*,i.sample_rate,t.staging_path,t.accepted_frames,t.durable_frames,"
        "t.publication_sha256,t.publication_frames,t.publication_bytes "
        "FROM audio_segments s JOIN capture_intervals i USING(interval_id) "
        "LEFT JOIN segment_storage t USING(segment_id) ORDER BY s.session_id,s.sequence").fetchall()
    next_sequence, next_frame = {}, {}
    for row in rows:
        sid, segment_id, interval = row["session_id"], row["segment_id"], row["interval_id"]
        with store.transaction("recovery_order"):
            if row["sequence"] != next_sequence.get(sid, 0):
                _finding(store, sid, segment_id, "sequence_gap", "gap", row["relative_path"])
            expected_frame = next_frame.get(interval, 0)
            if row["start_frame"] != expected_frame:
                code = "frame_gap" if row["start_frame"] > expected_frame else "frame_overlap"
                _finding(store, sid, segment_id, code, "gap", row["relative_path"], missing=abs(row["start_frame"] - expected_frame))
        next_sequence[sid] = row["sequence"] + 1
        next_frame[interval] = row["start_frame"] + (row["accepted_frames"] if row["accepted_frames"] is not None else row["frame_count"])
        _repair(store, row)

    known = {row["relative_path"] for row in rows}
    known.update(row["staging_path"] for row in rows if row["staging_path"])
    # Reserve pending names only for sources that entered the repair flow. A
    # foreign repair-named file beside an intact original remains an orphan.
    known.update(f"audio/{row[0]}.recovered.wav" for row in store.db.execute(
        "SELECT segment_id FROM audio_segments WHERE state='unresolved'"))
    known.update(row[0] for row in store.db.execute("SELECT relative_path FROM segment_recoveries"))
    for path in sorted(artifact_path(store.root, "audio").iterdir()):
        relative = path.relative_to(store.root).as_posix()
        artifact_path(store.root, relative)  # Never follow unexpected redirects.
        if relative not in known:
            # A crash may leave a builder or a published repair before DB commit.
            with store.transaction("recovery_orphan"):
                _finding(store, None, None, "orphan_artifact", "uncertain", relative)

    with store.transaction("recovery_status"):
        for session in sessions:
            sid = session["session_id"]
            issue = store.db.execute("SELECT 1 FROM recovery_findings WHERE session_id=? AND severity<>'info' LIMIT 1", (sid,)).fetchone()
            status = "needs_attention" if issue else "clean"
            store.db.execute("UPDATE recording_sessions SET recovery_status=? WHERE session_id=? AND recovery_status<>?", (status, sid, status))
            if issue:
                store.db.execute("UPDATE processing_jobs SET state='blocked',lease_owner=NULL,lease_expires_at=NULL,error_code='recovery_required' WHERE session_id=? AND state NOT IN ('blocked','succeeded')", (sid,))
    # Keep historical evidence unchanged, but an adopted repair is no longer an
    # unrelated orphan. Its immutable recovery record resolves that classification.
    return [dict(row) for row in store.db.execute(
        "SELECT f.* FROM recovery_findings f WHERE NOT (f.code='orphan_artifact' "
        "AND EXISTS (SELECT 1 FROM segment_recoveries r WHERE r.relative_path=f.source_path)) "
        "ORDER BY f.finding_id")]
