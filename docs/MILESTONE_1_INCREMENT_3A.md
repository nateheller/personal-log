# Dedicated recorder process and control system

**WARNING: Audio and metadata are UNENCRYPTED. Use disposable synthetic test data
only. Do not record personal or sensitive content.**

## Scope and process boundaries

Increment 3A adds a host API and two spawned child processes. It adds no desktop
controls, real microphone, encryption, transcription, or AI executor. Theme code
is unchanged. The earlier increment documents describe their historical scope;
this document specifies the runtime orchestration now added around that storage.

The controller owns a `RecorderProcess` instance. `start()` returns only after the
worker opens its external root, runs startup recovery, and reports ready. It never
automatically starts or resumes capture. `request('record')`, `request('pause')`,
`request('resume')`, and `request('finish')` send commands and await acknowledgments.
`make_command`, `send`, `wait`, `poll`, `disconnect`, and `reconnect` expose explicit
retry and host-event-loop integration. The synchronous convenience API is for
tests/host code; a future Qt adapter must call it off the GUI thread or use an
asynchronous bridge. Python script entry points must use the usual
`if __name__ == '__main__':` guard for Windows spawn. No dependency on PySide6 is
introduced into recording/storage.
`wait()` retains other command acknowledgments and non-acknowledgment notifications
in bounded caches for later waits/polls, including interruption statuses delivered
in the same transport batch as the requested acknowledgment.

The supervisor process owns authenticated loopback IPC, controller ownership,
command serialization, heartbeat policy, worker liveness, and hard-stop deadlines.
It never opens the database or waits for disk writes. Its OS parent handle tracks
the original controller process identity, avoiding PID-reuse-based liveness checks.

The worker process has a paced synthetic capture/control loop and one storage
thread. Only the storage thread owns SegmentStore and SQLite. PCM and control
barriers travel through a bounded FIFO; capture never waits for space, storage,
status readers, or IPC writes. Ordinary commands first undergo storage-side receipt
lookup/state validation. Only a prepared Pause/Finish stops capture and inserts a
barrier after all previously accepted audio. Duplicate or invalid commands therefore
cannot inadvertently pause a newer interval. A full capture queue stops capture,
reports storage failure, and begins bounded preservation rather than dropping audio
silently or allocating unbounded memory.

This extra supervisor is necessary because a watchdog thread sharing a writer's
interpreter cannot enforce termination if that interpreter stalls. Tests also stall
the whole worker loop to exercise that boundary. There is no restart loop: a dead
worker stops its supervisor, and a new host start performs recovery explicitly.

## IPC and ownership

The new `ipc.v2.json` is separate from the unchanged v1 boundary schema. Runtime
uses only v2 commands/statuses. Messages have schema version, UUID message ID, UTC
creation time, session ID, and a typed payload. The host chooses a session UUID
before Record; supplying an existing entry ID creates a supplemental session.
After Finish, another Record with a new session UUID can run in the same processes.

Only the configured controller UUID and a fresh per-launch 256-bit token authorize
commands. The listener binds to 127.0.0.1 on an ephemeral port. Tokens are carried
only over local IPC and not stored in the application database. This is local
control authentication, not encryption or protection against an administrator or
same-user memory inspection. Audio never travels over IPC; only commands, receipts,
states, counts, and sanitized error codes do.

Transport uses nonblocking, newline-delimited JSON, no pickle. Frames are limited
to 16 KiB, input/output buffers to 128 KiB, connections to four, pending commands to
32, and work per poll is bounded. Unauthenticated partial handshakes expire. Wrong
tokens, malformed/deep/oversized JSON, unknown schema fields, and invalid operations
cannot renew the lease. A stalled reader is disconnected when its bounded output
buffer fills; capture continues under the same heartbeat deadline.

New commands use increasing controller sequence numbers and stable command IDs.
Successful lifecycle receipts are immutable SQLite records introduced by numbered
migration 003. They contain the canonical command digest and original response.
The receipt commits with its state/events; Finish includes its queue job. An exact
retry returns the saved response, including after a process restart. Reusing an ID
with changed fields is rejected. Unknown stale sequences are receipt lookups only,
never executed as new operations. Pending duplicate IDs are coalesced. A bounded
supervisor cache accelerates retries; older receipts remain available in SQLite.

An acknowledgment is evidence of the command's historical result, not a current
state snapshot. For example, replaying an old Record receipt after startup recovery
does not reactivate capture, even though that receipt says recording. Hosts must
use ready/snapshot statuses for live state. On a new launch, sequence allocation
starts after the owner's last durable lifecycle receipt; volatile heartbeat history
is scoped to the new authenticated transport lifetime.

Heartbeats are volatile control messages, not database work. Only a fresh sequence
for the current session renews the lease. Retrying the same heartbeat before expiry
can replay its old acknowledgment but does not renew it. After expiry, even that
retry is rejected. Ownership checks precede sequence reservation and heartbeat
renewal. Normal commands remain serialized while heartbeats bypass disk work.
Shutdown is a special fail-stop control signal: it can escalate an in-flight normal
stop, and repeated notifications never extend the existing preservation deadline.

## Lifecycle and transactions

Record commits the entry/session/interval, recording transition/event, and receipt,
then starts the fake source. Pause stops capture, drains accepted blocks, finalizes
the short segment, and atomically commits pausing/paused events, interval closure,
and its receipt. Resume creates a new timestamped interval and resuming/recording
events atomically with its receipt. Segment sequence continues across intervals;
start_frame resets within each interval. The synthetic signal itself continues
across Pause/Resume so tests can verify exact sample ordering.

Finish stops capture, drains, finalizes/validates/publishes segments, then commits
finalizing/finished events, interval closure, clean recovery status, receipt, and
one pending `process_session` job together. Job uniqueness is enforced by session,
input revision, job type, and pipeline version (`capture-v1`). A new Finish command
for the same already-finished clean session may add a receipt but cannot duplicate
the job. Job execution is deliberately absent. Failed or interrupted finalization
does not enqueue a job or claim Saved. Future processing must perform its normal
input validation, including empty audio handling.

Finish acknowledgment does not rehash the whole session after the durable commit;
each segment was already validated before publication. Explicit effective-reference
queries retain Increment 2's full validation behavior. This keeps acknowledgment
latency from growing with the entire journal session.

Interruption flags and deadlines are checked immediately before the Finish commit,
after injectable pre-commit I/O hooks. If observed before that guard, failure rolls
back state/events, job, and receipt. SQLite commit is the durable linearization
point: a Finish already committed cannot be retroactively cancelled or turned into
an unsaved recording. A shutdown notification in transit or arriving during the
kernel commit cannot provide an absolute cancellation ordering guarantee. The
supervisor still enforces the hard deadline if commit or finalization blocks.

## Deadlines and failure behavior

Default budgets are inherited from the approved ControlLease policy:

| Condition | Capture/preservation behavior |
| --- | --- |
| Temporary socket loss or stalled UI | Continue until 10 seconds from the last accepted heartbeat; reconnecting alone does not renew it. |
| Heartbeat expiry | Stop accepting audio, drain/finalize for at most five more seconds, then terminate the worker. Late heartbeats cannot restart it. |
| Confirmed controller-process death | Stop immediately and attempt interruption preservation for at most five seconds. |
| Normal shutdown | Stop capture immediately, attempt durable Finish and one queue job within five seconds, then exit. |
| Windows shutdown notification | Stop immediately and attempt interruption preservation within two seconds or the earlier existing deadline. |
| Capture/queue/storage failure | Stop capture; attempt preservation for at most five seconds, then terminate if blocked. |
| Worker crash | Report recorder_crash and exit; next startup recovery reconciles abandoned state. |

The supervisor expects worker ticks within two seconds. A lifecycle command that
does not complete within five seconds is treated as storage failure and gets the
bounded preservation budget, rather than waiting indefinitely while heartbeats
continue. Startup recovery has a separate 30-second default watchdog and never
starts audio. Shorter test timings are explicit RecorderConfig values; grace/drain/
Windows budgets cannot exceed the approved 10/5/2-second limits through config.

Normal shutdown can be escalated to UI/Windows interruption without extending its
deadline. The worker consults the latest latched reason before preserving. A blocked
writer or whole-loop stall cannot disable the supervisor, which requests process
termination at the deadline and bounds its joins. OS scheduling/termination latency
and storage hardware remain outside a Python real-time guarantee.

The worker exits if its private supervisor channel closes, avoiding an invisible
orphan recorder after supervisor failure. Startup errors allow supervisor cleanup
to finish before the controller resorts to force termination. A hard kill cannot
always persist the precise stopping reason; next startup may correctly report the
conservative recorder_crash fallback rather than the supervisor's known reason.

Preservation is best effort when the device or storage is failing. Audio with durable
frame intent can be reconciled by Increment 2 recovery. Captured blocks still queued,
or a block that failed before/partway through intent creation, are counted separately
from confirmed accepted_frames. If metadata remains writable, known unrepresented
frames become buffered_capture_gap findings; if an intent count cannot be confirmed,
capture_intent_unconfirmed is explicitly uncertain. Hard termination may prevent
that final accounting; unknown capture-tail findings remain the fallback. No claim
is made that unwritable or RAM-only audio survives a power failure or forced kill.

## Synthetic tests and remaining qualification

All worker task handoffs handle a full queue without blocking capture. Saturation
stops capture and requests interruption; frames read before a rejected audio
handoff remain in capture-gap accounting. A stop request latches its deadline
immediately and occupies at most one pending slot until the existing FIFO can
accept it. A later command result cannot restart capture after saturation.
The supervisor marks an internal stop as sent only after channel acceptance,
retrying rejected messages (including stricter shutdown escalation) with the
original shutdown command and without extending the preservation deadline.
Synthetic spawned-process regressions reject prepare, execute, audio, and stop
tasks, and reject supervisor stop messages both transiently and permanently.

Tests use only externally stored synthetic audio and real spawned processes. They
cover startup/exit, rapid and duplicate commands, conflicting IDs, ownership,
sequence rejection, disconnection/reconnection, unread statuses, heartbeat expiry,
late/replayed heartbeats, controller process death, worker crash, blocked writers,
whole-loop stalls, startup recovery without capture, exact frame/segment order,
Pause/Resume intervals, repeated Finish, durable receipt replay after commit/crash,
and rollback of queue/receipt/events together. IPC tests include malformed tokens,
partial/deep/oversized input and bounded output backpressure. Fake-only speed and
one-shot failure hooks make cross-boundary and pre-commit failures reproducible;
they cannot be selected over IPC and are not microphone features.

Run `.venv/Scripts/python.exe -m pytest -q` for the entire suite. All prior migration,
path, durability, recovery, contract, and theme tests remain applicable. Migration
003 upgrades the database to v3 without editing migrations 001 or 002. No real
audio, private metadata, diagnostic content, or database is a repository fixture.

Native Windows console logoff/shutdown notifications are wired through
SetConsoleCtrlHandler; an authenticated host can also forward shutdown explicitly.
A future native desktop host must integrate WM_QUERYENDSESSION/WM_ENDSESSION and
verify actual Windows shutdown/sleep behavior. This increment does not qualify that
desktop path, a real audio callback/driver, or responsiveness under GPU/AI workloads.
The existing Capture protocol is the adapter seam; a real source must provide
bounded/nonblocking handoff and cannot perform storage in its device callback.

Other limits remain: genuine power-loss testing, hardware flush honesty, filesystem
path races/hard links/cloud paths, no disk-space warning monitor, retained staging
growth, and unencrypted storage. Startup recovery of a large archive may exceed its
watchdog and require explicit host retry with a larger startup budget. The prototype
is not suitable for sensitive journals or an unattended production recorder.
