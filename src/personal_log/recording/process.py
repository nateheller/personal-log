"""UI-independent supervisor and a small synchronous controller API.

Loopback transport is token-authenticated JSON. The supervisor never opens the
audio database; its deadline loop remains independent of worker storage and UI.
"""

from collections import OrderedDict, deque
from dataclasses import dataclass
import hmac
import math
import multiprocessing as mp
import secrets
import socket
import threading
import time
from uuid import UUID, uuid4

from .contracts import ControlLease
from .ipc import JsonChannel, command, digest, status, validate_ipc
from personal_log.storage.paths import prepare_data_root


@dataclass(frozen=True)
class RecorderConfig:
    sample_rate: int = 48_000
    block_seconds: float = 0.1
    queue_blocks: int = 50
    grace_seconds: float = 10.0
    drain_seconds: float = 5.0
    windows_seconds: float = 2.0
    operation_timeout: float = 5.0
    worker_timeout: float = 2.0
    startup_timeout: float = 30.0
    # Explicit synthetic failure controls; never populated from IPC input.
    fault_boundary: str | None = None
    fault_mode: str = "fail"
    capture_fail_at: int | None = None
    fault_delay: float = 0.5
    fault_after: int = 1
    synthetic_speed: float = 1.0
    capture_source: str = 'fake'
    input_device: int | None = None
    input_device_name: str | None = None
    input_host_api: str | None = None

    def __post_init__(self):
        if self.capture_source not in {'fake', 'microphone'}:
            raise ValueError('Unknown capture source')
        if self.capture_source == 'microphone':
            if type(self.input_device) is not int or self.input_device < 0 or not self.input_device_name or not self.input_host_api:
                raise ValueError('Explicit microphone index, name, and host API required')
            if self.synthetic_speed != 1 or self.capture_fail_at is not None:
                raise ValueError('Synthetic pacing/failure controls cannot alter microphone capture')
        for value in (self.block_seconds, self.grace_seconds, self.drain_seconds, self.windows_seconds,
                      self.operation_timeout, self.worker_timeout, self.startup_timeout, self.fault_delay, self.synthetic_speed):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Recorder timing must be finite and positive")
        if type(self.sample_rate) is not int or not 1 <= self.sample_rate <= 192_000:
            raise ValueError("Invalid synthetic sample rate")
        if self.block_seconds > 1 or type(self.queue_blocks) is not int or not 2 <= self.queue_blocks <= 1000:
            raise ValueError("Invalid bounded capture queue")
        if self.fault_mode not in {"fail", "block", "crash", "delay"}:
            raise ValueError("Invalid synthetic fault")
        if type(self.fault_after) is not int or self.fault_after < 1 or self.synthetic_speed > 100:
            raise ValueError("Invalid synthetic test pacing")
        if self.grace_seconds > 10 or self.drain_seconds > 5 or self.windows_seconds > 2:
            raise ValueError("Configured control deadlines cannot exceed approved budgets")
        if self.capture_fail_at is not None and (type(self.capture_fail_at) is not int or self.capture_fail_at < 0):
            raise ValueError("Invalid synthetic capture failure boundary")


def _supervisor(ready, repository, root, owner, token, config, emergency):
    from .worker import worker_main

    parent = mp.parent_process()  # OS process creation identity, not PID polling.
    local, remote = socket.socketpair()
    worker = mp.get_context("spawn").Process(target=worker_main, args=(remote, config, repository, root, owner))
    worker.start()
    remote.close()
    channel = JsonChannel(local)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.setblocking(False)
    clients = []
    pending = deque()
    inflight = None
    inflight_at = 0
    cache = OrderedDict()
    last_sequence = -1
    live_state, live_sid = "idle", None
    captured = written = findings = 0
    lease = None
    startup = last_tick = time.monotonic()
    stop_sent = None
    stop_message = None
    stop_requests = OrderedDict()
    exit_at = None
    last_snapshot = 0
    shutdown_signal = threading.Event()
    callback = None
    startup_failed = False
    if __import__("os").name == "nt":
        import ctypes
        # Console hosts receive these notifications without a desktop UI. Native
        # desktop WM_QUERYENDSESSION integration belongs to a future app host.
        callback = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)(
            lambda event: (shutdown_signal.set() or True) if event in (5, 6) else False)
        ctypes.windll.kernel32.SetConsoleCtrlHandler(callback, True)

    def broadcast(message):
        for client in clients:
            if client["authenticated"] and not client["channel"].queue(message):
                client["channel"].close()  # A stalled reader cannot grow memory.

    def snapshot(code="snapshot"):
        return status(live_state, code, session_id=live_sid, next_sequence=last_sequence + 1,
                      captured_frames=captured, written_frames=written,
                      recovery_findings=findings, worker_pid=worker.pid)

    def reject(message, code):
        broadcast(status(live_state, "rejected", session_id=message["session_id"],
                         sequence=message["payload"]["sequence"], command_id=message["message_id"], error_code=code))

    def remember(message, response):
        cache[message["message_id"]] = (digest(message), response)
        cache.move_to_end(message["message_id"])
        while len(cache) > 256:
            cache.popitem(last=False)

    def stop(reason, now, message=None):
        nonlocal stop_sent, stop_message
        if lease is None:
            return
        if message is not None and len(stop_requests) < 32:
            stop_requests[message["message_id"]] = message
        stop_message = message or stop_message
        if reason in {"normal_shutdown", "ui_crash", "windows_shutdown"}:
            lease.shutdown(now, reason)
            if reason == "windows_shutdown":
                lease.drain_seconds = min(lease.drain_seconds, max(0, now - lease.stop_at) + config.windows_seconds)
        else:
            lease.poll(now)
            if lease.stop_at is None:
                lease.stop_at, lease.reason = now, reason
            elif lease.reason == "normal_shutdown":
                lease.reason = reason
        specification = (lease.reason, lease.stop_at + lease.drain_seconds)
        if stop_sent != specification:
            if channel.queue({"type": "stop", "reason": lease.reason, "deadline": specification[1], "message": stop_message}):
                stop_sent = specification

    try:
        while True:
            now = time.monotonic()
            for event in channel.poll():
                kind = event["type"]
                last_tick = now
                if kind == "ready":
                    lease = ControlLease(now, config.grace_seconds, config.drain_seconds)
                    last_sequence = event["next_sequence"] - 1
                    findings = event["recovery_findings"]
                    ready.send({"port": listener.getsockname()[1], "next_sequence": last_sequence + 1,
                                "worker_pid": worker.pid, "recovery_findings": findings})
                    ready.close()
                elif kind == "tick":
                    live_state, live_sid = event["state"], event["session_id"]
                    captured, written = event["captured"], event["written"]
                elif kind == "response":
                    response = event["response"]
                    if not event["replayed"] and event.get("action") != "finish_already":
                        live_state, live_sid = response["payload"]["state"], response["session_id"]
                    if inflight:
                        remember(inflight, response)
                    broadcast(response)
                    inflight = None
                elif kind == "fault":
                    if lease is None:
                        ready.send({"error": "startup_failed"})
                        startup_failed = True
                        break
                    if event.get("message"):
                        reject(event["message"], event["reason"])
                    stop(event["reason"], now)
                elif kind == "stopped":
                    live_state = event["state"]
                    for request in stop_requests.values():
                        response = event.get("response")
                        if response is None or response["payload"].get("command_id") != request["message_id"]:
                            response = status(live_state, "ack", session_id=request["session_id"],
                                sequence=request["payload"]["sequence"], command_id=request["message_id"])
                        remember(request, response)
                        broadcast(response)
                    code = "stopped" if live_state in {"finished", "idle"} else "interrupted"
                    error_reason = event.get("reason", lease.reason)
                    if error_reason == "normal_shutdown":
                        error_reason = "storage_failed"
                    fields = {} if code == "stopped" else {"error_code": error_reason}
                    broadcast(status(live_state, code, session_id=live_sid, **fields))
                    exit_at = now + 0.15

            if startup_failed:
                break
            if lease is None:
                if not worker.is_alive() or now - startup >= config.startup_timeout:
                    ready.send({"error": "startup_failed"})
                    break
            else:
                if not parent.is_alive():
                    stop("ui_crash", now)
                if emergency.is_set():
                    stop("ui_crash", now)
                if shutdown_signal.is_set():
                    stop("windows_shutdown", now)
                decision = lease.poll(now)
                if decision != "continue" and stop_sent != (lease.reason, lease.stop_at + lease.drain_seconds):
                    stop(lease.reason, now)
                if now - last_tick > config.worker_timeout or (inflight and now - inflight_at > config.operation_timeout):
                    stop("storage_failed", now)
                if lease.poll(now) == "force_stop" and worker.is_alive():
                    worker.terminate()
                    error_reason = "storage_failed" if lease.reason == "normal_shutdown" else lease.reason
                    broadcast(status("interrupted", "interrupted", session_id=live_sid, error_code=error_reason))
                    exit_at = now + 0.15
                if not worker.is_alive() and exit_at is None:
                    broadcast(status("interrupted", "interrupted", session_id=live_sid, error_code="recorder_crash"))
                    exit_at = now + 0.15

            # Accept and read only bounded work per iteration; never block on UI.
            for _ in range(4):
                try:
                    sock, _ = listener.accept()
                except BlockingIOError:
                    break
                if len(clients) >= 4:
                    sock.close()
                else:
                    clients.append({"channel": JsonChannel(sock), "authenticated": False, "opened": now})
            for client in clients:
                transport = client["channel"]
                for envelope in transport.poll():
                    supplied = envelope.get("token") if isinstance(envelope, dict) else None
                    if (not isinstance(supplied, str) or len(supplied) != len(token) or not supplied.isascii()
                            or not hmac.compare_digest(supplied, token)):
                        transport.close()
                        break
                    if not client["authenticated"]:
                        client["authenticated"] = True
                        transport.queue(snapshot("ready"))
                    message = envelope.get("message")
                    if message is None:
                        continue  # Authentication alone never renews the lease.
                    try:
                        validate_ipc(message)
                        if message["kind"] != "command":
                            raise ValueError("Expected a command")
                    except Exception:
                        transport.queue(status(live_state, "rejected", session_id=live_sid, error_code="invalid_message"))
                        continue
                    payload = message["payload"]
                    now = time.monotonic()
                    if payload["controller_id"] != owner:
                        reject(message, "not_owner")
                        continue
                    if payload["operation"] == "heartbeat" and (lease is None or lease.poll(now) != "continue"):
                        reject(message, "shutting_down")
                        continue
                    cached = cache.get(message["message_id"])
                    if cached:
                        if cached[0] != digest(message):
                            reject(message, "command_conflict")
                        else:
                            broadcast(cached[1])  # In particular, no heartbeat renewal.
                        continue
                    active = ([inflight] if inflight else []) + [item[0] for item in pending]
                    duplicate = next((m for m in active if m["message_id"] == message["message_id"]), None)
                    if duplicate:
                        if digest(duplicate) != digest(message):
                            reject(message, "command_conflict")
                        continue
                    op = payload["operation"]
                    stale = payload["sequence"] <= last_sequence
                    if op == "shutdown" and lease is not None:
                        if stale:
                            reject(message, "stale_sequence")
                        elif live_sid and message["session_id"] != live_sid:
                            reject(message, "wrong_session")
                        else:
                            last_sequence = payload["sequence"]
                            stop(payload.get("reason", "normal_shutdown"), now, message)
                        continue
                    if lease is None or lease.poll(now) != "continue":
                        reject(message, "shutting_down")
                        continue
                    if op == "heartbeat":
                        if stale:
                            reject(message, "stale_sequence")
                        elif live_sid and message["session_id"] != live_sid:
                            reject(message, "wrong_session")
                        else:
                            last_sequence = payload["sequence"]
                            if lease.heartbeat(now):
                                response = status(live_state, "ack", session_id=message["session_id"],
                                    sequence=payload["sequence"], command_id=message["message_id"])
                                remember(message, response)
                                broadcast(response)
                        continue
                    if len(pending) >= 32:
                        reject(message, "busy")
                        continue
                    last_sequence = max(last_sequence, payload["sequence"])
                    pending.append((message, stale))
            clients = [c for c in clients if not c["channel"].closed]
            for client in clients:
                if not client["authenticated"] and now - client["opened"] > 2:
                    client["channel"].close()
            if lease is not None and lease.stop_at is None and inflight is None and pending:
                inflight, lookup_only = pending.popleft()
                if not channel.queue({"type": "command", "message": inflight, "lookup_only": lookup_only}):
                    stop("storage_failed", now)
                inflight_at = now
            if lease is not None and now - last_snapshot >= 0.1:
                broadcast(snapshot())
                last_snapshot = now
            if exit_at is not None and now >= exit_at:
                break
            time.sleep(0.005)
    finally:
        if worker.is_alive():
            worker.terminate()
        worker.join(timeout=1)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=1)
        channel.close()
        listener.close()
        for client in clients:
            client["channel"].close()
        ready.close()
        if callback is not None:
            import ctypes
            ctypes.windll.kernel32.SetConsoleCtrlHandler(callback, False)


class RecorderProcess:
    """A test/host API, not a desktop UI. Calls have bounded controller timeouts."""

    def __init__(self, *, repository, root=None, controller_id=None, config=None):
        self.config = config or RecorderConfig()
        self.owner = controller_id or str(uuid4())
        if str(UUID(self.owner)) != self.owner:
            raise ValueError("Invalid controller ID")
        self.root = prepare_data_root(repository=repository, root=root)
        self.repository = str(repository)
        self.token = secrets.token_hex(32)
        self.session_id = str(uuid4())
        self.sequence = 0
        self.channel = None
        self.process = None
        self.events = deque(maxlen=1024)
        self.notifications = deque(maxlen=1024)
        self.responses = OrderedDict()
        self.descriptor = None
        self._emergency = None

    def start(self):
        context = mp.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        self._emergency = context.Event()
        self.process = context.Process(target=_supervisor, args=(sender, self.repository, str(self.root), self.owner, self.token, self.config, self._emergency))
        self.process.start()
        sender.close()
        try:
            if not receiver.poll(self.config.startup_timeout + 5):
                raise TimeoutError("Recorder startup timed out")
            self.descriptor = receiver.recv()
            if "error" in self.descriptor:
                raise RuntimeError("Recorder startup failed")
            self.sequence = self.descriptor["next_sequence"]
            self.reconnect()
            return self
        except BaseException:
            # An error descriptor is sent before supervisor cleanup completes.
            # Do not kill its finally block and strand the worker/root lock.
            self.process.join(timeout=3)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(timeout=1)
            raise
        finally:
            receiver.close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()

    def disconnect(self):
        if self.channel:
            self.channel.close()
            self.channel = None

    def reconnect(self):
        self.disconnect()
        sock = socket.create_connection(("127.0.0.1", self.descriptor["port"]), timeout=1)
        self.channel = JsonChannel(sock)
        self._send_envelope({"token": self.token})

    def make_command(self, operation, *, session_id=None, **parameters):
        message = command(operation, self.owner, session_id or self.session_id, self.sequence, **parameters)
        self.sequence += 1
        return message

    def _send_envelope(self, envelope):
        if not self.channel or self.channel.closed or not self.channel.queue(envelope):
            raise ConnectionError("Recorder connection unavailable")
        until = time.monotonic() + 1
        while self.channel.outgoing:
            self.events.extend(self.channel.poll())
            if self.channel.closed or time.monotonic() >= until:
                raise ConnectionError("Recorder send timed out")
            time.sleep(0.002)

    def send(self, message):
        self._send_envelope({"token": self.token, "message": message})

    def _read(self):
        if self.channel:
            for event in self.channel.poll():
                self.events.append(validate_ipc(event))
        events = list(self.events)
        self.events.clear()
        return events

    def poll(self):
        events = list(self.notifications)
        self.notifications.clear()
        events.extend(self._read())
        return events

    def wait(self, command_id, *, timeout=10, maintain_lease=True):
        if command_id in self.responses:
            return self.responses.pop(command_id)
        until = time.monotonic() + timeout
        next_heartbeat = time.monotonic() + min(1.0, self.config.grace_seconds / 3)
        while time.monotonic() < until:
            matched = None
            for event in self._read():
                response_id = event["payload"].get("command_id")
                if response_id == command_id:
                    matched = event
                elif response_id:
                    self.responses[response_id] = event
                    while len(self.responses) > 256:
                        self.responses.popitem(last=False)
                else:
                    self.notifications.append(event)
            if matched is not None:
                return matched
            if maintain_lease and time.monotonic() >= next_heartbeat:
                self.send(self.make_command("heartbeat"))
                next_heartbeat = time.monotonic() + min(1.0, self.config.grace_seconds / 3)
            if self.process and not self.process.is_alive():
                raise ConnectionError("Recorder stopped before acknowledgment")
            time.sleep(0.005)
        raise TimeoutError("Recorder command timed out")

    def request(self, operation, *, timeout=10, **parameters):
        if operation == "record" and parameters.get("session_id"):
            self.session_id = parameters["session_id"]
        message = self.make_command(operation, **parameters)
        self.send(message)
        return self.wait(message["message_id"], timeout=timeout)

    def close(self):
        if self.process and self.process.is_alive():
            try:
                if self.channel is None or self.channel.closed:
                    self.reconnect()
                message = self.make_command("shutdown")
                self.send(message)
                response = self.wait(message["message_id"], timeout=self.config.drain_seconds + 1, maintain_lease=False)
                if response["payload"]["code"] != "ack":
                    self._emergency.set()
            except (ConnectionError, TimeoutError, OSError):
                self._emergency.set()
            self.process.join(timeout=self.config.drain_seconds + 2)
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(timeout=1)
        self.disconnect()
