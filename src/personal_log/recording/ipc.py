"""Bounded JSON transport; no pickle, audio, or filesystem operations on IPC."""

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from importlib.resources import files
import json
import socket
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker


MAX_FRAME = 16_384
MAX_BUFFER = 131_072


def _validator():
    checker = FormatChecker()

    @checker.checks("date-time", raises=ValueError)
    def timestamp(value):
        if not isinstance(value, str):
            return True
        datetime.fromisoformat(value)
        return value.endswith("Z")

    return Draft202012Validator(json.loads(files(__package__).joinpath("ipc.v2.json").read_text()), format_checker=checker)


_VALIDATOR = _validator()


def validate_ipc(message):
    _VALIDATOR.validate(message)
    if message["kind"] == "command":
        payload = message["payload"]
        if ("reason" in payload and payload["operation"] != "shutdown") or ("entry_id" in payload and payload["operation"] != "record"):
            raise ValueError("Unexpected operation parameters")
    return deepcopy(message)


def digest(message):
    return sha256(json.dumps(message, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def command(operation, controller_id, session_id, sequence, **parameters):
    return validate_ipc({"schema_version": 2, "kind": "command", "message_id": str(uuid4()),
                         "session_id": session_id, "created_at": now(),
                         "payload": {"operation": operation, "controller_id": controller_id, "sequence": sequence, **parameters}})


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def status(state, code, *, session_id=None, sequence=0, command_id=None, **fields):
    payload = {"state": state, "code": code, "sequence": sequence, **fields}
    if command_id is not None:
        payload["command_id"] = command_id
    return validate_ipc({"schema_version": 2, "kind": "status", "message_id": str(uuid4()),
                         "session_id": session_id, "created_at": now(), "payload": payload})


class JsonChannel:
    """Every poll is nonblocking, with bounded work, frames and output memory."""

    def __init__(self, sock):
        self.sock = sock
        sock.setblocking(False)
        self.incoming = bytearray()
        self.outgoing = bytearray()
        self.closed = False

    def close(self):
        self.closed = True
        self.sock.close()

    def queue(self, value):
        frame = json.dumps(value, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if len(frame) > MAX_FRAME or len(self.outgoing) + len(frame) > MAX_BUFFER:
            return False
        self.outgoing.extend(frame)
        return True

    def poll(self):
        if self.closed:
            return []
        try:
            if self.outgoing:
                try:
                    sent = self.sock.send(self.outgoing)
                    del self.outgoing[:sent]
                except BlockingIOError:
                    pass
            try:
                data = self.sock.recv(MAX_FRAME)
            except BlockingIOError:
                data = None
            if data == b"":
                self.close()
                return []
            if data:
                self.incoming.extend(data)
            if len(self.incoming) > MAX_BUFFER:
                raise ValueError("Oversized input")
            messages = []
            for _ in range(16):
                boundary = self.incoming.find(b"\n")
                if boundary < 0:
                    if len(self.incoming) > MAX_FRAME:
                        raise ValueError("Oversized frame")
                    break
                if boundary > MAX_FRAME:
                    raise ValueError("Oversized frame")
                frame = bytes(self.incoming[:boundary])
                del self.incoming[:boundary + 1]
                messages.append(json.loads(frame))
            return messages
        except (OSError, ValueError, UnicodeError, RecursionError):
            self.close()
            return []
