from itertools import product
from uuid import UUID

import pytest
from jsonschema import ValidationError

from personal_log.recording.contracts import (
    ControlLease, DISPOSABLE_WARNING, State, TRANSITIONS, transition, validate_contract,
)


def message(kind="command"):
    return {
        "schema_version": 1, "kind": kind,
        "message_id": str(UUID(int=1)), "session_id": str(UUID(int=2)),
        "created_at": "2026-10-15T12:00:00Z",
        "payload": ({"operation": "record", "controller_id": str(UUID(int=3)), "sequence": 0}
                    if kind == "command" else
                    {"state": "starting", "code": "state_changed", "sequence": 0}),
    }


@pytest.mark.parametrize("kind", ["command", "event"])
def test_valid_contract_is_independent(kind):
    value = message(kind)
    result = validate_contract(value)
    assert result == value
    result["payload"]["sequence"] = 99
    assert value["payload"]["sequence"] == 0


@pytest.mark.parametrize("key,value", [
    ("schema_version", 2), ("schema_version", True), ("session_id", "bad"),
    ("message_id", "bad"), ("kind", "unknown"),
    ("session_id", "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"),
    ("created_at", "2026-02-30T12:00:00Z"),
    ("created_at", "2026-10-15T12:00:00+00:00"), ("extra", "not allowed"),
])
def test_invalid_contract(key, value):
    item = message()
    item[key] = value
    with pytest.raises(ValidationError):
        validate_contract(item)


@pytest.mark.parametrize("payload", [
    {}, {"operation": "transcribe"},
    {"operation": "record", "controller_id": str(UUID(int=3)), "sequence": -1},
    {"operation": "record", "controller_id": str(UUID(int=3)), "sequence": True},
    {"operation": "record", "controller_id": str(UUID(int=3)), "sequence": 0, "audio": "no"},
])
def test_invalid_command_payload(payload):
    item = message()
    item["payload"] = payload
    with pytest.raises(ValidationError):
        validate_contract(item)


@pytest.mark.parametrize("current,target", list(product(State, repeat=2)))
def test_all_lifecycle_pairs(current, target):
    if target in TRANSITIONS[current]:
        assert transition(current, target) == target
    else:
        with pytest.raises(ValueError):
            transition(current, target)


def test_warning_is_explicit():
    assert "UNENCRYPTED" in DISPOSABLE_WARNING
    assert "disposable test data only" in DISPOSABLE_WARNING


def test_temporary_ui_loss_does_not_interrupt_and_heartbeat_renews():
    lease = ControlLease(100)
    assert lease.poll(109.99) == "continue"
    assert lease.heartbeat(109.99)
    assert lease.poll(119.98) == "continue"
    assert lease.poll(119.99) == "interrupt"
    assert lease.reason == "control_timeout"
    assert lease.poll(124.99) == "force_stop"


def test_late_heartbeat_cannot_revive_expired_capture():
    lease = ControlLease(0)
    assert not lease.heartbeat(10)
    assert not lease.heartbeat(11)
    assert lease.poll(15) == "force_stop"


def test_delayed_poll_does_not_extend_deadlines():
    assert ControlLease(0).poll(100) == "force_stop"


@pytest.mark.parametrize("reason,action,budget", [
    ("normal_shutdown", "finish", 5),
    ("ui_crash", "interrupt", 5),
    ("windows_shutdown", "interrupt", 2),
])
def test_shutdown_reasons(reason, action, budget):
    lease = ControlLease(0)
    assert lease.shutdown(1, reason) == action
    assert not lease.heartbeat(1.5)
    assert lease.poll(1 + budget) == "force_stop"


def test_repeated_shutdown_does_not_extend_budget():
    lease = ControlLease(0)
    lease.shutdown(1, "ui_crash")
    lease.shutdown(4, "normal_shutdown")
    assert lease.reason == "ui_crash"
    assert lease.poll(6) == "force_stop"


@pytest.mark.parametrize("reason,budget", [("ui_crash", 5), ("windows_shutdown", 3)])
def test_failure_escalates_normal_shutdown(reason, budget):
    lease = ControlLease(0)
    assert lease.shutdown(0, "normal_shutdown") == "finish"
    assert lease.shutdown(1, reason) == "interrupt"
    assert lease.reason == reason
    assert lease.poll(budget) == "force_stop"


def test_windows_shutdown_does_not_extend_an_earlier_stop_deadline():
    lease = ControlLease(0)
    lease.shutdown(0, "ui_crash")
    lease.shutdown(4, "windows_shutdown")
    assert lease.poll(5) == "force_stop"


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_monotonic_times(value):
    with pytest.raises(ValueError):
        ControlLease(value)
    with pytest.raises(ValueError):
        ControlLease(0).poll(value)


def test_clock_cannot_move_backwards():
    lease = ControlLease(0)
    lease.poll(4)
    with pytest.raises(ValueError):
        lease.heartbeat(3)
