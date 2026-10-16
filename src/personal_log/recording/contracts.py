"""Versioned JSON boundary contracts and deterministic lifecycle policies."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from importlib.resources import files
import json
import math

from jsonschema import Draft202012Validator, FormatChecker


SCHEMA_VERSION = 1
DISPOSABLE_WARNING = (
    "WARNING: Recordings are UNENCRYPTED. Use disposable test data only. "
    "Do not record personal or sensitive content."
)


class State(StrEnum):
    STARTING = "starting"
    RECORDING = "recording"
    PAUSING = "pausing"
    PAUSED = "paused"
    RESUMING = "resuming"
    FINALIZING = "finalizing"
    FINISHED = "finished"
    INTERRUPTED = "interrupted"
    RECOVERY_REQUIRED = "recovery_required"


# Idle belongs to the controller: a session exists only after Record.
TRANSITIONS = {
    State.STARTING: {State.RECORDING, State.INTERRUPTED},
    State.RECORDING: {State.PAUSING, State.FINALIZING, State.INTERRUPTED},
    State.PAUSING: {State.PAUSED, State.INTERRUPTED},
    State.PAUSED: {State.RESUMING, State.FINALIZING, State.INTERRUPTED},
    State.RESUMING: {State.RECORDING, State.INTERRUPTED},
    State.FINALIZING: {State.FINISHED, State.INTERRUPTED},
    State.FINISHED: set(),
    State.INTERRUPTED: {State.RECOVERY_REQUIRED},
    State.RECOVERY_REQUIRED: {State.PAUSED, State.FINALIZING},
}


def transition(current: State, target: State) -> State:
    """Validate a transition; completion states require persistence acknowledgments."""
    current, target = State(current), State(target)
    if target not in TRANSITIONS[current]:
        raise ValueError(f"Invalid recording transition: {current} -> {target}")
    return target


def _validator() -> Draft202012Validator:
    schema = json.loads(files(__package__).joinpath("contracts.v1.json").read_text())
    checker = FormatChecker()

    # jsonschema's date-time check otherwise depends on optional format extras.
    @checker.checks("date-time", raises=ValueError)
    def utc_timestamp(value):
        if not isinstance(value, str):
            return True  # The schema's type check reports non-string values.
        datetime.fromisoformat(value)
        return value.endswith("Z")

    return Draft202012Validator(schema, format_checker=checker)


_VALIDATOR = _validator()


def validate_contract(message: dict) -> dict:
    """Reject unknown versions/fields and return an independent validated value."""
    _VALIDATOR.validate(message)
    return deepcopy(message)


@dataclass
class ControlLease:
    """Recorder-owned policy; times are finite monotonic seconds, never wall time.

    Missing heartbeats allow 10 seconds from the last acknowledged heartbeat.
    Once stopping is latched, late heartbeats cannot restart recording. The
    recording.process supervisor provides runtime enforcement independently of I/O.
    """

    last_heartbeat: float
    grace_seconds: float = 10.0
    drain_seconds: float = 5.0
    stop_at: float | None = None
    reason: str | None = None
    _observed_at: float | None = None

    def __post_init__(self):
        for value in (self.last_heartbeat, self.grace_seconds, self.drain_seconds):
            if not math.isfinite(value) or value < 0:
                raise ValueError("Lease times must be finite and nonnegative")
        if not self.grace_seconds or not self.drain_seconds:
            raise ValueError("Lease durations must be positive")
        if self.stop_at is not None or self.reason is not None or self._observed_at is not None:
            raise ValueError("A lease must start without a stop decision")
        self._observed_at = self.last_heartbeat

    def _observe(self, now: float):
        if not math.isfinite(now) or now < self._observed_at:
            raise ValueError("Monotonic time must not go backwards")
        self._observed_at = now

    def poll(self, now: float) -> str:
        self._observe(now)
        if self.stop_at is None and now >= self.last_heartbeat + self.grace_seconds:
            self.stop_at = self.last_heartbeat + self.grace_seconds
            self.reason = "control_timeout"
        if self.stop_at is None:
            return "continue"
        if now >= self.stop_at + self.drain_seconds:
            return "force_stop"
        return "finish" if self.reason == "normal_shutdown" else "interrupt"

    def heartbeat(self, now: float) -> bool:
        if self.poll(now) != "continue":
            return False
        self.last_heartbeat = now
        return True

    def shutdown(self, now: float, reason: str) -> str:
        if reason not in {"normal_shutdown", "ui_crash", "windows_shutdown"}:
            raise ValueError("Unknown shutdown reason")
        self.poll(now)
        if self.stop_at is None:
            self.stop_at, self.reason = now, reason
        elif reason in {"ui_crash", "windows_shutdown"} and self.reason == "normal_shutdown":
            # Failure escalates an orderly finish; never extend its existing budget.
            self.reason = reason
        if reason == "windows_shutdown":
            # Keep the budget relative to the original stop decision, even when
            # its deadline has passed. Clamp it without extending that deadline;
            # repeated/late shutdown notices cannot grant more preservation time.
            elapsed = max(0.0, now - self.stop_at)
            self.drain_seconds = max(0.0, min(self.drain_seconds, elapsed + 2.0))
        return self.poll(now)
