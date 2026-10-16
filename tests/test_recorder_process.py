from pathlib import Path
from copy import deepcopy
import json
import socket
import sqlite3
import struct
import subprocess
import sys
import time
from uuid import uuid4
import wave

import pytest

from personal_log.recording.ipc import JsonChannel, MAX_BUFFER, command, validate_ipc
from personal_log.recording.process import RecorderConfig, RecorderProcess
from personal_log.storage.segments import SegmentStore


def config(**overrides):
    values = dict(sample_rate=100, block_seconds=0.02, queue_blocks=20,
                  grace_seconds=1.5, drain_seconds=1, windows_seconds=0.3,
                  operation_timeout=1.5, worker_timeout=1, startup_timeout=10)
    values.update(overrides)
    return RecorderConfig(**values)


def test_process_record_pause_resume_finish(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config()) as recorder:
        assert recorder.process.pid != recorder.descriptor["worker_pid"]
        assert query(root, "SELECT count(*) FROM recording_sessions") == [(0,)]
        assert recorder.request("record")["payload"]["state"] == "recording"
        time.sleep(0.15)
        assert recorder.request("pause")["payload"]["state"] == "paused"
        assert recorder.request("resume")["payload"]["state"] == "recording"
        time.sleep(0.15)
        assert recorder.request("finish")["payload"]["state"] == "finished"
        sid = recorder.session_id
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.db.execute("SELECT state FROM recording_sessions").fetchone()[0] == "finished"
        assert store.db.execute("SELECT count(*) FROM capture_intervals").fetchone()[0] == 2
        assert store.db.execute("SELECT count(*) FROM processing_jobs").fetchone()[0] == 1
        assert len(store.segments(sid)) == 2
        pcm = b""
        for ref in store.segments(sid):
            with wave.open(str(root / ref["relative_path"]), "rb") as source:
                pcm += source.readframes(source.getnframes())
        assert pcm == b"".join(struct.pack("<h", (n % 65536) - 32768) for n in range(len(pcm) // 2))


def spin(recorder, predicate, *, timeout=4, heartbeat=True):
    until = time.monotonic() + timeout
    next_heartbeat = 0
    seen = []
    while time.monotonic() < until:
        if heartbeat and time.monotonic() >= next_heartbeat:
            recorder.send(recorder.make_command("heartbeat"))
            next_heartbeat = time.monotonic() + min(0.2, recorder.config.grace_seconds / 3)
        seen.extend(recorder.poll())
        if predicate(seen):
            return seen
        if not recorder.process.is_alive():
            break
        time.sleep(0.01)
    assert predicate(seen), seen[-4:]
    return seen


def await_frames(recorder, minimum=10):
    return spin(recorder, lambda events: any(e["payload"].get("written_frames", 0) >= minimum for e in events))


def await_exit(recorder, timeout=4, heartbeat=False):
    until = time.monotonic() + timeout
    next_heartbeat = 0
    events = []
    while time.monotonic() < until and recorder.process.is_alive():
        if heartbeat and time.monotonic() >= next_heartbeat and recorder.channel and not recorder.channel.closed:
            try:
                recorder.send(recorder.make_command("heartbeat"))
            except ConnectionError:
                pass
            next_heartbeat = time.monotonic() + 0.1
        events.extend(recorder.poll())
        time.sleep(0.01)
    recorder.process.join(timeout=0.5)
    events.extend(recorder.poll())
    assert not recorder.process.is_alive(), events[-4:]
    return events


def query(root, sql):
    db = sqlite3.connect(root / "recording.sqlite3")
    try:
        return db.execute(sql).fetchall()
    finally:
        db.close()


def test_rapid_commands_and_duplicate_retries_are_serialized(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config()) as recorder:
        messages = [recorder.make_command(op) for op in ("record", "pause", "resume", "finish")]
        for message in messages:
            recorder.send(message)
            recorder.send(message)
        replies = [recorder.wait(m["message_id"])["payload"] for m in messages]
        assert [r["state"] for r in replies] == ["recording", "paused", "recording", "finished"]
        recorder.send(messages[-1])
        assert recorder.wait(messages[-1]["message_id"])["payload"] == replies[-1]
        assert recorder.request("finish")["payload"]["state"] == "finished"
    assert query(root, "SELECT count(*) FROM journal_entries") == [(1,)]
    assert query(root, "SELECT count(*) FROM recording_sessions") == [(1,)]
    assert query(root, "SELECT count(*) FROM capture_intervals") == [(2,)]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(1,)]


def test_ownership_sequences_and_conflicting_ids(external, checkout):
    with RecorderProcess(repository=checkout, root=external / "data", config=config()) as recorder:
        message = recorder.make_command("record")
        recorder.send(message)
        first = recorder.wait(message["message_id"])
        recorder.send(message)
        assert recorder.wait(message["message_id"]) == first
        changed = deepcopy(message)
        changed["payload"]["operation"] = "pause"
        recorder.send(changed)
        assert recorder.wait(message["message_id"])["payload"]["error_code"] == "command_conflict"
        intruder = recorder.make_command("pause")
        intruder["payload"]["controller_id"] = str(uuid4())
        recorder.send(intruder)
        assert recorder.wait(intruder["message_id"])["payload"]["error_code"] == "not_owner"
        stale = recorder.make_command("pause")
        stale["payload"]["sequence"] = 0
        recorder.send(stale)
        assert recorder.wait(stale["message_id"])["payload"]["error_code"] == "stale_sequence"
        wrong = recorder.make_command("pause", session_id=str(uuid4()))
        recorder.send(wrong)
        assert recorder.wait(wrong["message_id"])["payload"]["error_code"] == "wrong_session"
        assert recorder.request("pause")["payload"]["state"] == "paused"
        assert recorder.request("finish")["payload"]["state"] == "finished"


def test_multiple_sessions_and_supplemental_entry(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config()) as recorder:
        recorder.request("record")
        await_frames(recorder)
        recorder.request("finish")
        entry = query(root, "SELECT entry_id FROM journal_entries")[0][0]
        recorder.request("record", session_id=str(uuid4()), entry_id=entry)
        await_frames(recorder)
        recorder.request("finish")
    assert query(root, "SELECT count(*) FROM journal_entries") == [(1,)]
    assert query(root, "SELECT count(*) FROM recording_sessions") == [(2,)]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(2,)]


def test_temporary_disconnect_reconnect_and_stalled_reads_do_not_stop_capture(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(grace_seconds=2)) as recorder:
        recorder.request("record")
        await_frames(recorder)
        before = query(root, "SELECT sum(accepted_frames) FROM segment_storage")[0][0]
        # The controller performs no IPC reads/writes during either interval.
        time.sleep(0.2)
        recorder.disconnect()
        time.sleep(0.25)
        after = query(root, "SELECT sum(accepted_frames) FROM segment_storage")[0][0]
        assert after > before
        assert query(root, "SELECT state FROM recording_sessions") == [("recording",)]
        recorder.reconnect()
        assert recorder.request("heartbeat")["payload"]["code"] == "ack"
        recorder.request("finish")
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.recover() == []


def test_heartbeat_expiry_rejects_late_and_duplicate_heartbeats(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(grace_seconds=0.45, drain_seconds=0.4,
                         fault_boundary="session_interrupt.before_commit", fault_mode="delay", fault_delay=0.25)) as recorder:
        recorder.request("record")
        heartbeat = recorder.make_command("heartbeat")
        recorder.send(heartbeat)
        recorder.wait(heartbeat["message_id"])
        time.sleep(0.3)
        # An exact retry is acknowledged historically but must not renew its lease.
        recorder.send(heartbeat)
        recorder.wait(heartbeat["message_id"], maintain_lease=False)
        time.sleep(0.2)
        recorder.send(heartbeat)
        assert recorder.wait(heartbeat["message_id"], timeout=1, maintain_lease=False)["payload"]["error_code"] == "shutting_down"
        late = recorder.make_command("heartbeat")
        recorder.send(late)
        response = recorder.wait(late["message_id"], timeout=1, maintain_lease=False)
        assert response["payload"]["error_code"] == "shutting_down"
        await_exit(recorder, timeout=2)
    assert query(root, "SELECT state,interruption_code FROM recording_sessions") == [("interrupted", "control_timeout")]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]


@pytest.mark.parametrize("reason,state,jobs", [("normal_shutdown", "finished", 1), ("ui_crash", "interrupted", 0), ("windows_shutdown", "interrupted", 0)])
def test_explicit_shutdown_policies_preserve_audio(external, checkout, reason, state, jobs):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(windows_seconds=0.8)) as recorder:
        recorder.request("record")
        await_frames(recorder, 10)
        response = recorder.request("shutdown", reason=reason)
        assert response["payload"]["state"] == state
        await_exit(recorder)
    assert query(root, "SELECT state FROM recording_sessions") == [(state,)]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(jobs,)]
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        assert sum(s["frame_count"] for s in store.segments(recorder.session_id)) >= 10


@pytest.mark.parametrize("fault_boundary", ["pcm.before_write", "session_finish.before_commit"])
def test_storage_failure_never_enqueues_or_reports_success(external, checkout, fault_boundary):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary=fault_boundary)) as recorder:
        recorder.request("record")
        if fault_boundary == "session_finish.before_commit":
            await_frames(recorder)
            response = recorder.request("finish")
            assert response["payload"]["code"] == "rejected"
        await_exit(recorder, timeout=3)
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]
    assert query(root, "SELECT count(*) FROM recording_sessions WHERE state='finished'") == [(0,)]
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.recover()


def test_blocked_writer_is_terminated_by_independent_supervisor(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary="pcm.flushed", fault_mode="block",
                         queue_blocks=4, drain_seconds=0.4, grace_seconds=3)) as recorder:
        recorder.request("record")
        events = await_exit(recorder, timeout=4, heartbeat=True)
        assert any(e["payload"].get("error_code") == "storage_failed" for e in events)
    with SegmentStore(repository=checkout, root=root) as store:
        findings = store.recover()
        assert any(f["code"] == "capture_tail_unknown" for f in findings)
        assert sum(s["frame_count"] for s in store.segments(recorder.session_id)) >= 100


def test_recorder_crash_then_startup_recovery_does_not_restart_capture(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary="pcm.flushed", fault_mode="crash", grace_seconds=3)) as first:
        first.request("record")
        await_exit(first, timeout=4, heartbeat=True)
        sid = first.session_id
    with RecorderProcess(repository=checkout, root=root, config=config()) as second:
        assert second.descriptor["recovery_findings"] > 0
        before = query(root, "SELECT count(*),sum(accepted_frames) FROM segment_storage")
        time.sleep(0.15)
        assert query(root, "SELECT count(*),sum(accepted_frames) FROM segment_storage") == before
        assert query(root, "SELECT state FROM recording_sessions") == [("recovery_required",)]
    with SegmentStore(repository=checkout, root=root) as store:
        assert sum(s["frame_count"] for s in store.segments(sid)) >= 100


def test_finish_commit_crash_replays_durable_receipt_and_one_job(external, checkout):
    root = external / "data"
    owner = str(uuid4())
    with RecorderProcess(repository=checkout, root=root, controller_id=owner,
                         config=config(fault_boundary="session_finish.committed", fault_mode="crash")) as first:
        first.request("record")
        await_frames(first)
        finish = first.make_command("finish")
        first.send(finish)
        await_exit(first, timeout=3)
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(1,)]
    with RecorderProcess(repository=checkout, root=root, controller_id=owner, config=config()) as second:
        second.send(finish)
        response = second.wait(finish["message_id"])
        assert response["payload"]["state"] == "finished"
        assert query(root, "SELECT count(*) FROM processing_jobs") == [(1,)]
        assert query(root, "SELECT count(*) FROM recording_sessions") == [(1,)]


def test_duplicate_record_receipt_after_crash_never_recaptures(external, checkout):
    root = external / "data"
    owner = str(uuid4())
    with RecorderProcess(repository=checkout, root=root, controller_id=owner,
                         config=config(fault_boundary="pcm.written", fault_mode="crash")) as first:
        record = first.make_command("record")
        first.send(record)
        first.wait(record["message_id"])
        await_exit(first, timeout=3)
    with RecorderProcess(repository=checkout, root=root, controller_id=owner, config=config()) as second:
        before = query(root, "SELECT * FROM segment_storage")
        second.send(record)
        response = second.wait(record["message_id"])
        assert response["payload"]["state"] == "recording"  # Historical receipt.
        time.sleep(0.1)
        assert query(root, "SELECT * FROM segment_storage") == before
        assert query(root, "SELECT state FROM recording_sessions") == [("recovery_required",)]


def test_startup_failure_releases_worker_and_root_lock(external, checkout):
    root = external / "data"
    with SegmentStore(repository=checkout, root=root):
        recorder = RecorderProcess(repository=checkout, root=root, config=config(startup_timeout=1))
        with pytest.raises(RuntimeError, match="startup failed"):
            recorder.start()
        assert not recorder.process.is_alive()
    with SegmentStore(repository=checkout, root=root):
        pass


def test_controller_process_death_interrupts_without_waiting_for_heartbeat(external, checkout):
    root = external / "data"
    script = """
import os, sys, time
from pathlib import Path
from personal_log.recording.process import RecorderProcess, RecorderConfig
if __name__ == '__main__':
    cfg = RecorderConfig(sample_rate=100,block_seconds=.02,grace_seconds=10,drain_seconds=.5)
    rec = RecorderProcess(repository=Path(sys.argv[1]),root=Path(sys.argv[2]),config=cfg).start()
    rec.request('record')
    time.sleep(.15)
    os._exit(9)
"""
    began = time.monotonic()
    result = subprocess.run([sys.executable, "-c", script, str(checkout), str(root)], capture_output=True, text=True, timeout=12)
    assert result.returncode == 9, result.stderr
    assert time.monotonic() - began < 8  # Includes two spawn startups; grace is 10s.
    until = time.monotonic() + 3
    while query(root, "SELECT state FROM recording_sessions") != [("interrupted",)] and time.monotonic() < until:
        time.sleep(.02)
    assert query(root, "SELECT interruption_code FROM recording_sessions") == [("ui_crash",)]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()


def test_ipc_validation_and_bounded_backpressure():
    message = command("record", str(uuid4()), str(uuid4()), 0)
    assert validate_ipc(message) == message
    for key, value in [("schema_version", 99), ("session_id", None)]:
        invalid = deepcopy(message)
        invalid[key] = value
        with pytest.raises(Exception):
            validate_ipc(invalid)
    first, second = socket.socketpair()
    channel = JsonChannel(first)
    try:
        count = 0
        while channel.queue(message):
            count += 1
        assert count > 0
        assert len(channel.outgoing) <= MAX_BUFFER
    finally:
        channel.close()
        second.close()


def test_windows_shutdown_escalates_normal_stop_without_finishing(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root,
                         config=config(fault_boundary="session_finish.before_commit", fault_mode="delay", fault_delay=0.6,
                                       windows_seconds=0.9, drain_seconds=1.5)) as recorder:
        recorder.request("record")
        await_frames(recorder)
        normal = recorder.make_command("shutdown")
        recorder.send(normal)
        # Wait until audio publication completes; Finish is then delayed before
        # its guarded commit, leaving the durable session state recording.
        until = time.monotonic() + 1
        while not query(root, "SELECT 1 FROM audio_segments WHERE state='finalized'") and time.monotonic() < until:
            time.sleep(.01)
        assert query(root, "SELECT state FROM recording_sessions") == [("recording",)]
        windows = recorder.make_command("shutdown", reason="windows_shutdown")
        recorder.send(windows)
        response = recorder.wait(windows["message_id"], timeout=3, maintain_lease=False)
        assert response["payload"]["state"] == "interrupted"
        await_exit(recorder, timeout=2)
    assert query(root, "SELECT state,interruption_code FROM recording_sessions") == [("interrupted", "windows_shutdown")]
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]


def test_whole_worker_loop_stall_cannot_disable_supervisor(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary="worker_loop", fault_mode="block",
                         worker_timeout=0.3, drain_seconds=0.4, grace_seconds=3)) as recorder:
        recorder.request("record")
        events = await_exit(recorder, timeout=3, heartbeat=True)
        assert any(e["payload"].get("error_code") == "storage_failed" for e in events)
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.recover()


def test_unowned_and_malformed_commands_do_not_renew_lease(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(grace_seconds=.4, drain_seconds=.4)) as recorder:
        recorder.request("record")
        until = time.monotonic() + .55
        while time.monotonic() < until and recorder.process.is_alive():
            message = recorder.make_command("heartbeat")
            message["payload"]["controller_id"] = str(uuid4())
            invalid = recorder.make_command("heartbeat")
            invalid["schema_version"] = 99
            try:
                recorder.send(message)
                recorder.send(invalid)
            except ConnectionError:
                # Expiry can close the channel while the supervisor is still
                # alive flushing its final status. The durable reason below
                # remains the assertion that invalid traffic did not renew it.
                break
            recorder.poll()
            time.sleep(.03)
        await_exit(recorder, timeout=2)
    assert query(root, "SELECT interruption_code FROM recording_sessions") == [("control_timeout",)]


@pytest.mark.parametrize("fault_after", [1, 2])
def test_failed_creation_accounts_for_capture_not_represented_by_intent(external, checkout, fault_after):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root, config=config(
            block_seconds=.3, synthetic_speed=50, fault_boundary="segment_intent.before_commit",
            fault_after=fault_after, queue_blocks=50, grace_seconds=3)) as recorder:
        recorder.request("record")
        await_exit(recorder, timeout=5, heartbeat=True)
    gap = query(root, "SELECT missing_frames FROM recovery_findings WHERE code='buffered_capture_gap'")
    assert gap and gap[0][0] >= (30 if fault_after == 1 else 10)
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        refs = store.segments(recorder.session_id)
        assert sum(r["frame_count"] for r in refs) == (0 if fault_after == 1 else 2000)


def test_process_segment_rotation_keeps_exact_sample_continuity(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root,
                         config=config(block_seconds=.3, synthetic_speed=20, queue_blocks=50, grace_seconds=3)) as recorder:
        recorder.request("record")
        await_frames(recorder, 4100)
        recorder.request("finish")
    with SegmentStore(repository=checkout, root=root) as store:
        refs = store.segments(recorder.session_id)
        assert len(refs) >= 3
        assert [r["sequence"] for r in refs] == list(range(len(refs)))
        assert refs[0]["frame_count"] == refs[1]["frame_count"] == 2000
        pcm = b""
        for ref in refs:
            with wave.open(str(root / ref["relative_path"]), "rb") as source:
                pcm += source.readframes(source.getnframes())
        assert pcm == b"".join(struct.pack("<h", (n % 65536) - 32768) for n in range(len(pcm) // 2))


def test_malformed_unauthenticated_connections_cannot_stall_or_crash_recorder(external, checkout):
    with RecorderProcess(repository=checkout, root=external / "data", config=config()) as recorder:
        malformed = [
            json.dumps({"token": "\u00e9" * 64}).encode() + b"\n",
            b"[" * 2000 + b"0" + b"]" * 2000 + b"\n",
            b"x" * 17_000 + b"\n",
        ]
        for frame in malformed:
            sock = socket.create_connection(("127.0.0.1", recorder.descriptor["port"]), timeout=1)
            sock.sendall(frame)
            sock.close()
            time.sleep(.03)
        # A half-written handshake remains open while legitimate control works.
        slow = socket.create_connection(("127.0.0.1", recorder.descriptor["port"]), timeout=1)
        try:
            slow.sendall(b'{"token":')
            recorder.request("record")
            await_frames(recorder)
            assert recorder.request("finish")["payload"]["state"] == "finished"
        finally:
            slow.close()


@pytest.mark.parametrize("parameters", [{"grace_seconds": 11}, {"drain_seconds": 6}, {"windows_seconds": 3},
    {"block_seconds": 0}, {"queue_blocks": 1}, {"sample_rate": True}, {"capture_fail_at": -1},
    {"fault_after": 0}, {"synthetic_speed": float("inf")}])
def test_invalid_recorder_configuration_is_rejected(parameters):
    with pytest.raises(ValueError):
        RecorderConfig(**parameters)


def test_failed_normal_shutdown_reports_interruption_and_no_job(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root,
                         config=config(fault_boundary="session_finish.before_commit")) as recorder:
        recorder.request("record")
        await_frames(recorder)
        response = recorder.request("shutdown", reason="normal_shutdown")
        assert response["payload"]["state"] == "interrupted"
        events = await_exit(recorder)
        assert any(e["payload"].get("error_code") == "storage_failed" for e in events)
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]
    assert query(root, "SELECT state,interruption_code FROM recording_sessions") == [("interrupted", "storage_failed")]


def test_blocked_normal_shutdown_reports_force_stop_without_protocol_error(external, checkout):
    root = external / "data"
    with RecorderProcess(repository=checkout, root=root,
                         config=config(fault_boundary="session_finish.before_commit", fault_mode="block", drain_seconds=.4)) as recorder:
        recorder.request("record")
        await_frames(recorder)
        recorder.send(recorder.make_command("shutdown"))
        events = await_exit(recorder, timeout=3)
        assert any(e["payload"].get("error_code") == "storage_failed" for e in events)
        assert recorder.process.exitcode == 0
    assert query(root, "SELECT count(*) FROM processing_jobs") == [(0,)]
    with SegmentStore(repository=checkout, root=root) as store:
        assert store.recover()


def test_wait_preserves_non_acknowledgment_notifications(external, checkout):
    from personal_log.recording.ipc import status

    recorder = RecorderProcess(repository=checkout, root=external / "data", config=config())
    message = recorder.make_command("finish")
    ack = status("interrupted", "ack", session_id=recorder.session_id, command_id=message["message_id"])
    interruption = status("interrupted", "interrupted", session_id=recorder.session_id, error_code="storage_failed")
    recorder.events.extend([ack, interruption])
    assert recorder.wait(message["message_id"], maintain_lease=False) == ack
    assert recorder.poll() == [interruption]
