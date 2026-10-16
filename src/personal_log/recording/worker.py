"""Private recorder worker: paced fake capture and an independent storage thread.

The supervisor is a separate interpreter so a hung writer cannot disable its
deadline enforcement. This module never communicates directly with a UI socket.
"""

import json
import os
from pathlib import Path
import queue
import threading
import time

from .fake import AudioBlock, AudioFormat, FakeMicrophone
from .ipc import JsonChannel, digest, status
from personal_log.storage.files import DurableIO
from personal_log.storage.segments import PersistenceError, SegmentStore


class SyntheticIO(DurableIO):
    def __init__(self, config):
        self.config = config
        self.triggered = False
        self.seen = 0

    def hook(self, boundary):
        if not self.triggered and boundary == self.config.fault_boundary:
            self.seen += 1
            if self.seen < self.config.fault_after:
                return
            self.triggered = True
            if self.config.fault_mode == "block":
                threading.Event().wait()  # Supervisor must terminate this process.
            elif self.config.fault_mode == "crash":
                os._exit(81)
            elif self.config.fault_mode == "delay":
                time.sleep(self.config.fault_delay)
            else:
                raise OSError("Synthetic storage failure")


class CommandRejected(ValueError):
    pass


def _reply(message, state, code="ack", **fields):
    return status(state, code, session_id=message["session_id"],
                  sequence=message["payload"]["sequence"], command_id=message["message_id"], **fields)


def _storage_loop(tasks, results, shared, deadline, config, repository, root, owner):
    writer = None
    current_sid = None

    def guard():
        stopping = deadline[0]
        if stopping is not None and (stopping[0] != "normal_shutdown" or time.monotonic() >= stopping[1]):
            raise PersistenceError("Finish interrupted before commit")

    try:
        with SegmentStore(repository=Path(repository), root=Path(root), io=SyntheticIO(config)) as store:
            findings = store.recover()
            last = store.db.execute("SELECT coalesce(max(sequence),-1) FROM recorder_commands WHERE controller_id=?", (owner,)).fetchone()[0]
            results.put_nowait({"type": "ready", "next_sequence": last + 1, "recovery_findings": len(findings)})
            failed = False
            while True:
                task = tasks.get()
                kind = task["type"]
                if kind == "audio":
                    if not failed:
                        shared["attempted"] += task["block"].frames
                        try:
                            writer.append(task["block"])
                            shared["written"] += task["block"].frames
                            shared["accounted"] += task["block"].frames
                        except Exception:
                            try:
                                shared["accounted"] = store.db.execute(
                                    "SELECT coalesce(sum(t.accepted_frames),0) FROM segment_storage t "
                                    "JOIN audio_segments s USING(segment_id) WHERE s.session_id=?", (current_sid,)).fetchone()[0]
                            except Exception:
                                shared["accounting_exact"] = False
                            failed = True
                            results.put_nowait({"type": "fault", "reason": "storage_failed"})
                    continue
                if kind == "stop":
                    reason = deadline[0][0] if deadline[0] is not None else task["reason"]
                    message = task.get("message")
                    response = _reply(message, "finished" if reason == "normal_shutdown" else "interrupted") if message else None
                    receipt = (lambda: store.remember_command(message, response)) if message and current_sid else None
                    if current_sid:
                        existing = store.db.execute("SELECT state FROM recording_sessions WHERE session_id=?", (current_sid,)).fetchone()[0]
                        if reason == "normal_shutdown" and existing != "finished":
                            try:
                                if writer and not writer.closed:
                                    writer.finish(enqueue=True, receipt=receipt, guard=guard, return_references=False)
                                else:
                                    store.finish_paused(current_sid, receipt=receipt, guard=guard)
                            except PersistenceError:
                                reason = deadline[0][0] if deadline[0] is not None else "storage_failed"
                                if reason == "normal_shutdown":
                                    reason = "storage_failed"
                                store.interrupt(current_sid, reason)
                        elif existing != "finished":
                            if writer and not writer.closed:
                                writer.preserve_interruption(reason)
                            else:
                                store.interrupt(current_sid, reason)
                        existing = store.db.execute("SELECT state FROM recording_sessions WHERE session_id=?", (current_sid,)).fetchone()[0]
                        gap = shared["captured"] - shared["accounted"]
                        if gap > 0:
                            from personal_log.storage.recovery import _finding
                            with store.transaction("buffered_gap"):
                                code = "buffered_capture_gap" if shared["accounting_exact"] else "capture_intent_unconfirmed"
                                severity = "gap" if shared["accounting_exact"] else "uncertain"
                                _finding(store, current_sid, None, code, severity, missing=gap)
                    else:
                        existing = "idle"
                    if response:
                        # Normal shutdown of an already finished/idle session has
                        # no new capture effect; its response is transport-level.
                        if response["payload"]["state"] != existing:
                            response = _reply(message, existing)
                    results.put_nowait({"type": "stopped", "state": existing, "response": response, "reason": reason})
                    break

                message = task["message"]
                payload, sid = message["payload"], message["session_id"]
                op = payload["operation"]
                if kind == "prepare":
                    receipt = store.db.execute("SELECT * FROM recorder_commands WHERE command_id=?", (message["message_id"],)).fetchone()
                    if receipt:
                        if receipt["command_sha256"] != digest(message):
                            results.put_nowait({"type": "response", "response": _reply(message, "idle", "rejected", error_code="command_conflict"), "replayed": True})
                        else:
                            results.put_nowait({"type": "response", "response": json.loads(receipt["response_json"]), "replayed": True})
                        continue
                    try:
                        if task.get("lookup_only"):
                            raise CommandRejected("stale_sequence")
                        maximum = store.db.execute("SELECT coalesce(max(sequence),-1) FROM recorder_commands WHERE controller_id=?", (owner,)).fetchone()[0]
                        if payload["sequence"] <= maximum:
                            raise CommandRejected("stale_sequence")
                        row = store.db.execute("SELECT state,recovery_status FROM recording_sessions WHERE session_id=?", (sid,)).fetchone()
                        if op == "record":
                            if row is not None or (current_sid and store.db.execute("SELECT state FROM recording_sessions WHERE session_id=?", (current_sid,)).fetchone()[0] != "finished"):
                                raise CommandRejected("invalid_state")
                            action = "record"
                        elif row is None:
                            raise CommandRejected("wrong_session")
                        elif op == "finish" and row["state"] == "finished" and row["recovery_status"] == "clean":
                            action = "finish_already"
                        elif sid != current_sid:
                            raise CommandRejected("wrong_session")
                        elif (op, row["state"]) not in {("pause", "recording"), ("resume", "paused"), ("finish", "recording"), ("finish", "paused")}:
                            raise CommandRejected("invalid_state")
                        else:
                            action = op
                        results.put_nowait({"type": "prepared", "message": message, "action": action})
                    except CommandRejected as error:
                        results.put_nowait({"type": "response", "response": _reply(message, "idle", "rejected", error_code=str(error)), "replayed": True})
                    continue

                action = task["action"]
                target = {"record": "recording", "resume": "recording", "pause": "paused", "finish": "finished", "finish_already": "finished"}[action]
                response = _reply(message, target)
                receipt = lambda: store.remember_command(message, response)
                try:
                    if action == "record":
                        current_sid, interval = store.begin_session(audio_format=AudioFormat(sample_rate=config.sample_rate),
                            entry_id=payload.get("entry_id"), session_id=sid, receipt=receipt)
                        writer = store.writer(current_sid, interval)
                        shared["written"] = shared["attempted"] = shared["accounted"] = 0
                        shared["accounting_exact"] = True
                    elif action == "pause":
                        writer.pause(receipt=receipt)
                    elif action == "resume":
                        interval = store.resume_session(sid, receipt=receipt)
                        writer = store.writer(sid, interval)
                    elif action == "finish_already":
                        with store.transaction("finish_retry", guard):
                            store.enqueue(sid)
                            receipt()
                    elif writer and not writer.closed:
                        writer.finish(enqueue=True, receipt=receipt, guard=guard, return_references=False)
                    else:
                        store.finish_paused(sid, receipt=receipt, guard=guard)
                    results.put_nowait({"type": "response", "response": response, "replayed": False, "action": action})
                except Exception:
                    failed = True
                    results.put_nowait({"type": "fault", "reason": "storage_failed", "message": message})
    except Exception:
        results.put_nowait({"type": "fault", "reason": "storage_failed"})


def worker_main(sock, config, repository, root, owner):
    channel = JsonChannel(sock)
    tasks, results = queue.Queue(config.queue_blocks + 8), queue.Queue(128)
    shared = {"captured": 0, "written": 0, "attempted": 0, "accounted": 0, "accounting_exact": True}
    deadline = [None]
    storage = threading.Thread(target=_storage_loop, args=(tasks, results, shared, deadline, config, repository, root, owner), daemon=True)
    storage.start()
    capture = None
    capturing = False
    interval_position = 0
    state, sid = "idle", None
    next_capture = next_tick = time.monotonic()
    stopping = False
    task_saturated = False
    pending_fault = False
    pending_stop = None
    stop_enqueued = False
    frozen = False
    exit_at = None
    block_frames = max(1, int(config.sample_rate * config.block_seconds))

    def enqueue_task(task, *, fault_on_full=True):
        nonlocal capturing, task_saturated, pending_fault
        try:
            tasks.put_nowait(task)
            return True
        except queue.Full:
            if fault_on_full:
                capturing = False
                if capture and capture.running:
                    capture.stop()
                task_saturated = True
                pending_fault = True
            return False

    while True:
        now = time.monotonic()
        for incoming in channel.poll():
            if incoming["type"] == "command" and not stopping and not task_saturated:
                enqueue_task({"type": "prepare", "message": incoming["message"], "lookup_only": incoming.get("lookup_only", False)})
            elif incoming["type"] == "stop":
                reason, stop_deadline = incoming["reason"], incoming["deadline"]
                deadline[0] = (reason, stop_deadline)
                capturing = False
                if capture and capture.running:
                    capture.stop()
                stopping = True
                if not stop_enqueued:
                    pending_stop = incoming  # One bounded slot, behind accepted FIFO audio.
        for _ in range(16):
            try:
                result = results.get_nowait()
            except queue.Empty:
                break
            if result["type"] == "ready":
                channel.queue(result)
            elif result["type"] == "prepared":
                if stopping or task_saturated:
                    continue
                action = result["action"]
                if action in {"pause", "finish"}:
                    capturing = False
                    if capture and capture.running:
                        capture.stop()
                    state = "pausing" if action == "pause" else "finalizing"
                enqueue_task({"type": "execute", **{k: result[k] for k in ("message", "action")}})
            elif result["type"] == "response":
                if not result["replayed"]:
                    action = result["action"]
                    if action != "finish_already":
                        state = result["response"]["payload"]["state"]
                        sid = result["response"]["session_id"]
                    if action in {"record", "resume"} and not stopping and not task_saturated:
                        if action == "record":
                            capture = FakeMicrophone(AudioFormat(sample_rate=config.sample_rate), fail_at_frame=config.capture_fail_at)
                            shared["captured"] = 0
                        capture.start()
                        interval_position = 0
                        next_capture = now + config.block_seconds / config.synthetic_speed
                        capturing = True
                channel.queue(result)
            elif result["type"] == "fault":
                capturing = False
                if capture and capture.running:
                    capture.stop()
                channel.queue(result)
            elif result["type"] == "stopped":
                state = result["state"]
                channel.queue(result)
                exit_at = now + 0.1
        if pending_stop is not None and enqueue_task(pending_stop, fault_on_full=False):
            pending_stop = None
            stop_enqueued = True
        if pending_fault and channel.queue({"type": "fault", "reason": "storage_failed"}):
            pending_fault = False
        if capturing and now >= next_capture:
            if tasks.qsize() >= config.queue_blocks:
                capturing = False
                capture.stop()
                channel.queue({"type": "fault", "reason": "storage_failed"})
            else:
                try:
                    raw = capture.read(block_frames)
                    block = AudioBlock(interval_position, raw.frames, raw.pcm)
                    shared["captured"] += raw.frames
                    if not enqueue_task({"type": "audio", "block": block}):
                        continue  # Read frames remain accounted as an unrepresented gap.
                    interval_position += raw.frames
                    next_capture = now + raw.frames / config.sample_rate / config.synthetic_speed
                    if (not frozen and config.fault_boundary == "worker_loop" and config.fault_mode == "block"
                            and shared["captured"] >= 10):
                        frozen = True
                        threading.Event().wait()  # Synthetic whole-loop stall.
                except Exception:
                    capturing = False
                    channel.queue({"type": "fault", "reason": "capture_failed"})
        if now >= next_tick:
            channel.queue({"type": "tick", "state": state, "session_id": sid,
                           "captured": shared["captured"], "written": shared["written"]})
            next_tick = now + 0.05
        if exit_at is not None and now >= exit_at:
            channel.poll()
            break
        if channel.closed:
            # Supervisor disappeared: never let its worker become an orphan recorder.
            os._exit(82)
        time.sleep(0.005)
