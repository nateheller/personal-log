# Recording foundation contracts version 1

**WARNING: Recordings are UNENCRYPTED. Use disposable test data only. Do not record personal or sensitive content.**

Increment 1 has no recorder process, microphone integration, audio writer, or
desktop view. This warning is also exported as `DISPOSABLE_WARNING`; the first
recording view must display it persistently using semantic theme tokens. Existing
theme tokens are unchanged because no interface elements are added here.

## Boundaries and identities

The dedicated recorder process remains the approved runtime design. Recording,
storage, UI/themes, and future AI workers remain separate. This increment contains
only executable contracts, a deterministic fake source, path checks, and metadata
schema initialization. It does not launch a process or execute jobs.

Journal entries, recording sessions, capture intervals, and audio segments have
independent canonical lowercase UUIDs. An entry has many sessions; adding a
supplemental recording later creates another timestamped session with the same
entry ID. Pause/resume creates another interval within the session. A segment's
interval must belong to that same session. No text or audio is stored in SQLite.

SQLite schema v1 is separate from recorder JSON boundary schema v1 and the existing
provisional journal-entry export schema v0.1. The latter is left unchanged; it is
not the database schema. JSON boundary messages reject unknown versions/fields,
invalid UUIDs, non-UTC timestamps, and unknown operations/states. Messages carry
stable IDs and sequences; future IPC must enforce controller ownership, increasing
sequences, and command deduplication before acknowledging heartbeats. Contract
validation alone does not provide those runtime guarantees.

## Lifecycle

There is no persisted idle session. Record creates `starting`, then capture readiness
allows `recording`. Pause follows `recording -> pausing -> paused`; Resume follows
`paused -> resuming -> recording`. Finish follows `recording|paused -> finalizing
-> finished`. An active failure leads to `interrupted`, then `recovery_required`.
After reconciliation and explicit user action, recovery may return to `paused` or
`finalizing`. A finished session is terminal; supplemental audio gets a new session.
Completion transitions represent durable acknowledgments, not button clicks.

The transition function rejects repeated transitions; a future command dispatcher
must deduplicate command IDs before calling it. The database permits updates that
leave the state unchanged. Session state and lifecycle events must eventually be
written in the same transaction by the recording service; no service exists yet.

## Control and shutdown policy

All lease decisions use a recorder-owned monotonic clock. Heartbeats are expected
once per second; 10 seconds from the last accepted heartbeat is the hard grace
deadline. Repeated disconnect notices do not reset it. `ControlLease` models these
decisions deterministically; it has no dependency on UI event-loop progress.

| Situation | Required runtime behavior |
| --- | --- |
| Normal shutdown | Explicit command immediately stops accepting audio; drain and durably finalize within five seconds. Mark finished and enqueue only after durable success. Otherwise preserve and mark interrupted. |
| Temporary UI communication loss | Continue capture until the heartbeat deadline. Reconnect to the same controller/session and acknowledge a fresh heartbeat before expiry. No automatic microphone restart after expiry. |
| Confirmed UI crash | Immediately stop accepting audio; preserve/drain for at most five seconds, then exit interrupted. A broken pipe alone is treated as temporary communication loss, not proof of crash. |
| Heartbeat expiry | Stop accepting audio at the deadline, latch interruption, drain for at most five seconds, then force stop. Late heartbeats cannot revive the session. |
| Recorder crash | No surviving recorder can update its state. On next startup, an exclusive owner must mark abandoned active sessions interrupted with `recorder_crash`, then recovery_required and reconcile artifacts. Never restart capture automatically. |
| Windows shutdown | Stop accepting audio immediately, attempt preservation for at most two seconds or the earlier OS deadline, and exit interrupted. No shutdown veto or promise that the OS grants this time. |

The future recorder supervisor must enforce the drain/exit budget even if its writer
hangs. A blocked process cannot enforce a Python policy by itself. It must not rely
solely on the UI or a Python thread that shares a blocked interpreter. Actual
watchdog/process-liveness/Windows-shutdown integration is deferred and requires
failure tests before any real recording. Abandonment recovery provides the fallback
if hard termination prevents recording the final reason.

## Storage and migrations

Default data root: `%LOCALAPPDATA%/PersonalLog/DisposableCapture`. Missing or relative
LOCALAPPDATA fails closed. Explicit test roots must also be absolute and outside
the checkout. Reject repository overlap, existing symlink/junction aliases into
the checkout, UNC paths on Windows, redirected database files/sidecars, and traversal
file names. Never fall back to the working directory. Path checks do not eliminate
adversarial time-of-check/time-of-use races or detect every cloud-sync, mapped-drive,
or hard-link arrangement; arbitrary custom production paths are not qualified.

SQLite uses foreign keys, STRICT tables, WAL and synchronous FULL. Versioned SQL
is bundled as package data; migration DDL, checksum, and user_version are committed
together. Reopening verifies the migration checksum; newer schemas or unrecognized
nonempty databases are rejected. An exception rolls back the entire migration.
Do not edit an applied migration: add a new numbered migration in a future increment.

Constraints enforce identity shape, parent relationships, ordering uniqueness,
session transitions, one open interval per session, relative segment references,
finalized-segment integrity metadata, append-only lifecycle events, job identity,
and lease consistency. Job execution, atomic Finish/enqueue orchestration, segment
writing, snapshots, hashing, and recovery reconciliation remain future increments.
SQL timestamps require parseable UTC text; boundary JSON validation applies stricter
RFC3339 validation. Wall-clock changes can make end timestamps precede start times;
sample counts, interval identity, and sequence numbers govern audio ordering.

## Reproducible checks

Run `.venv/Scripts/python.exe -m pytest` from the checkout. Tests create only
synthetic metadata under external temporary directories, never audio/database files
inside the repository. They cover contract validation, all state pairs, lease
deadlines, fake capture interruption, migration rollback/process interruption,
foreign keys, supplemental sessions, and path redirection. These checks do not
prove audio durability or power-loss safety. Those require later segment-writing
fault injection, Windows process/device tests, and independent review.
