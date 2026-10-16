# Offline transcription foundation — Increment 4A

**WARNING: Audio, metadata, and transcripts are UNENCRYPTED. Use disposable
synthetic test data only. Do not process personal or sensitive recordings.**

## Integration proposal recorded before implementation

The authoritative Milestone 1 recorder is unchanged. Its Finish transaction marks
a session finished and creates one `processing_jobs` row (`process_session`,
`capture-v1`, session/input revision uniqueness). Recovery does not finish abandoned
recordings. This increment consumes that existing durable handoff; no second
recording queue, capture path, or automatic microphone action is introduced.

Add a transcription job linked one-to-one to an existing processing job. This
allows a richer explicit lifecycle without rebuilding the historical processing
table or changing the Finish transaction. Mirror coarse processing status for
compatibility; recovery's blocked jobs remain blocked. Only finished, clean,
matching-revision recordings are eligible. Materialization is idempotent.

Use independent SQLite connections and short `BEGIN IMMEDIATE` transactions for
claims, lease renewal, state changes, and result publication. Do not acquire the
recorder's lifetime root lock or hold a database write transaction during inference
or audio hashing. This permits recording and processing to coexist; SQLite remains
the authoritative queue. Per-attempt tokens fence late results after lease expiry
or ownership transfer. Retry budgets and retry times are persisted.

Validate the entire required segment set, never a filtered usable subset. Snapshot
session/entry/revision, segment and capture-interval identity/order, format, frame
counts, paths, hashes, and source provenance. Revalidate files and current metadata
before result publication and before whole-job completion. Require original
finalized artifacts for this increment: repaired/unresolved or ambiguous sources
need a future explicit eligibility policy, not automatic acceptance. No audio is
deleted, repaired, or modified by transcription.

Create an immutable original transcript header/source snapshot and immutable
successful per-segment text rows. Publish each success atomically with its attempt
status. On retry, preserve and reuse existing successful rows; complete the job
only when every required source has a successful result and still validates.
Failed/interrupted attempts retain sanitized error classifications separately.
Future derived transcripts must reference originals rather than overwrite them.

A public engine protocol accepts a validated open WAV stream, source metadata,
and a lease heartbeat callback. Use only an explicitly named deterministic fake
engine here. No faster-whisper import, model, download, network, UI, encryption,
retention, or refinement is included. Fake behavior belongs in its adapter, not
the production job orchestrator.

## Implementation and use

The `personal_log.transcription` package separates contracts, the fake engine,
source validation, SQLite persistence, and the synchronous worker. Importing it
never starts work. A host explicitly opens `TranscriptionStore(repository=checkout,
root=external_root)` and calls `TranscriptionWorker(store, engine).run_one()`.
No executor is attached to the recorder or GUI, and there is no automatic polling
loop. The return value is a durable job state, `None` when no matching eligible job
is ready, or `lost_claim` when output was fenced out. Publication/storage exceptions
propagate without a success acknowledgment. Each thread/process owns its own
connection; do not share a store across threads.

`FakeTranscriber` returns explicitly synthetic text derived deterministically from
source sequence and hash. Adapter-local scripts simulate retryable failure,
terminal failure, and interruption. The worker contains no fake-engine conditions.
No microphone, network access, telemetry, model download, or ASR library is needed.
Do not use the fake adapter to generate a purported transcript of a real recording.

## Accepted partial-result semantics

The accepted Increment 4A requirement is:

> Failed attempts cannot leave partially committed segment results or falsely
> completed transcripts. Successfully committed segments may remain durable for
> recovery, but the transcript cannot be marked complete until the entire
> validated source set has been processed.

Preserving a successful segment after a later failure is intentional. Atomicity
applies to each segment text/result and its successful attempt status together,
and separately to whole-job completion and parent-job success. It does not require
rolling back earlier successfully committed segments when a later segment fails.

| Term | Durable representation | Interpretation |
| --- | --- | --- |
| Durable intermediate transcription results | An immutable `original_transcripts` header/source snapshot and zero or more successfully committed `transcript_segments` rows, while the transcription job is not completed. | Recovery/retry evidence, not a complete original transcript. A segment's completed status describes only that segment. |
| Complete original transcript | The original header and the exact required source/result set, with the transcription job durably `completed` after full source validation and its parent processing job atomically `succeeded`. | The entire validated source set has been processed. No sentence assembly or refinement is implied. |
| Failed or blocked transcription work | A transcription job in `retryable_failure`, `interrupted`, or `terminal_failure`; the parent is `retryable_failure` for recoverable work or `blocked` for terminal work. Recording recovery can also block the parent. | Not a successful full transcript, even if immutable intermediate results exist. Retry reuses successful segments; terminal/blocked work is not automatically restarted. |

Consumers must use the transcription job state and complete source/result set,
not merely the existence of a header or one completed segment, to identify a
complete original transcript. A failed segment publication rolls back its text
and success status together. Earlier complete segment rows remain unchanged.
Neither failure nor retention of intermediate work permits falsely marking the
whole transcript complete. Originals are never overwritten or automatically deleted.

## Durable state machine

| Current state | Allowed next states | Meaning |
| --- | --- | --- |
| pending | running | Existing Finish job adopted; atomic claim creates its first attempt. |
| running | completed, retryable_failure, terminal_failure, interrupted | Only the current unexpired claim can publish; recovery may close an abandoned claim. |
| retryable_failure | running | Retry only when next_attempt_at is due and the attempt budget remains. |
| interrupted | running | A fresh claim may resume after an explicit interruption or expired-lease recovery. |
| completed | none | All required source results published and all inputs revalidated. |
| terminal_failure | none | No automatic retry, including exhausted attempt budgets. |

Contract validation rejects invalid transitions; SQL triggers enforce the same
state changes. Same-state SQL updates support lease renewal without changing claim
identity or attempt count. Claiming increments the attempt once and generates a
fresh token even when the worker owner name is reused. Defaults are three attempts,
a 60-second lease, and a 30-second retry delay. Attempt limit is frozen on job
creation (maximum 20); retry times and lease expiry are durable UTC timestamps.

Compatibility mapping to the existing processing job:

| Transcription state | Existing processing state |
| --- | --- |
| pending | pending |
| running | running (claim token is lease_owner) |
| completed | succeeded |
| retryable_failure / interrupted | retryable_failure |
| terminal_failure | blocked |

The original processing-job identity/uniqueness and recording Finish transaction
are unchanged. A previously blocked processing job is never automatically unblocked.
`succeeded` here denotes this increment's transcription work only; it does **not**
prove refinement, journal text validation, encryption, or seven-day retention
eligibility. Future pipeline stages need explicit completion contracts of their own.

## Schema and concurrency

Forward migration `004_transcription.sql` adds:

- `transcription_jobs`: one-to-one processing-job identity, frozen engine/model/
  configuration JSON, state, attempt budget, timestamps, owner/token, lease, retry
  schedule, and sanitized error code.
- `transcription_attempts` and `transcription_events`: attempt ownership, start/end
  state, errors, and append-only lifecycle evidence.
- `original_transcripts`: immutable original header, source recording/entry/revision,
  engine/model versions and configuration, complete source manifest/hash, timestamp,
  and original-kind/schema version.
- `transcript_sources`: immutable per-segment identity, order, interval, and exact
  provenance snapshot.
- `transcript_segment_attempts`: running/completed/failed/interrupted status and
  error information for each segment attempt.
- `transcript_segments`: immutable recognized text, successful status, timestamp,
  and producing attempt. Engine identity is available through its original header.

Closed attempt history and original header/source/text records reject update,
delete, and replacement. Successful segments are never overwritten on retry.
Original transcript headers may represent incomplete work; only a completed job
means the full source set succeeded. Empty segment text is allowed for silence;
a recording with no audio segments is an explicit `empty_recording` failure.

Database schema is v4; transcription contracts are v1 and recording IPC is still
v2. Migrations 001–003 are untouched. Schema-version/checksum inspection now uses
a consistent read snapshot, and migration rechecks occur inside `BEGIN IMMEDIATE`
to tolerate concurrent recorder/processor startup. DDL, checksum, and version still
commit together. A failed migration preserves the preceding complete schema.

Job materialization and claims serialize through short write transactions. A
claim also updates its parent lease and appends its attempt/event atomically.
Inference and source hashing hold no database write transaction and no recorder
root lock. A stalled engine therefore does not prevent another recording session
from being written. Shared disk/SQLite contention still requires qualification;
this is not a claim of unlimited throughput or zero latency on failing hardware.

## Authoritative input and result validation

Inputs are enumerated from the complete session tables, not `segments()`'s filtered
usable list. Require finished state, clean recovery status, matching input revision,
closed capture intervals, original finalized segments, no repairs/unresolved scoped
findings, continuous sequence/start-frame ordering, matching publication/accepted/
durable frame metadata, mono PCM16 format, exact byte counts, and SHA-256 hashes.
Empty capture intervals are allowed; missing required audio is not. Capture interval
boundaries remain visible and are not interpreted as sentence boundaries.

Snapshot every interval (including device identity/timestamps), every segment's
order/format/path/hash/frame metadata, the journal entry, and recording revision.
Before inference validate the path and actual opened file descriptor supplied to
the engine; hash that descriptor again afterward. Before publication compare
current metadata to the original snapshot. Before completion revalidate every
file, including segments with cached successful text, and compare the exact stored
source/result identity sets to the authoritative manifest. Source lists cannot be
truncated to produce apparent success. Store methods bind each original/source to
the current claim's job, rejecting cross-job results.

Only canonical validated WAVs are used; `.pcm.part`, builders, unknown artifacts,
missing/corrupt files, and recovery repairs are not silently substituted. Recovery
may preserve/reconstruct audio under Milestone 1 rules, but it does not authorize
an interrupted recording or ambiguous repair for transcription. Resolving those
cases requires a future explicit workflow. Source recordings remain unchanged.

## Publication, retry, and recovery

Per-segment text and successful segment-attempt state commit in one SQLite
transaction. Whole-job completion and parent success also commit atomically. There
are no transcript files to reconcile against SQLite. An exact result retry leaves
the original unchanged; conflicting text is rejected. Previously successful rows
survive later engine failure, SQLite failure, interruption, and recovery.

The engine may renew its lease through the provided callback. Renewal cannot
revive an expired claim. Token/ownership checks and pre-commit deadline guards
prevent stale output from publishing after another claim or a lease expiry observed
before commit. Transactions roll back on denied COMMIT, trigger failure, or a
pre-commit fault. If the process dies after commit but before acknowledging it,
the next worker discovers/reuses the durable rows without duplicates.

Each `run_one()` first reconciles abandoned claims, adopts eligible Finish jobs,
then claims at most one job for its configured identity. Recovery only closes
expired leases or claims whose parent was blocked by recording recovery; it does
not steal healthy leases. It marks unfinished segment attempts interrupted, keeps
successful text, and records errors. Exhaustion is terminal, including repeated
worker crashes. Retry delay and the finite budget avoid unbounded failure loops.
Explicit KeyboardInterrupt/SystemExit attempts to persist interruption and then
propagates; hard process termination relies on lease-expiry recovery instead.

Source/integrity errors and unexpected engine exceptions fail terminally. Engines
must deliberately classify transient failures as retryable. Error records contain
sanitized codes, not exception messages or speech. SQLite/I/O publication failures
propagate and leave the last durable state for recovery; callers must not treat an
exception as a completion acknowledgment. Database busy errors are not retried in
an unbounded loop.

## Verification and independent review

Run `.venv/Scripts/python.exe -m pytest -q`. All fixtures are disposable synthetic
audio/metadata in external temporary directories; no physical microphone, internet,
AI package, or model is required. Coverage includes lifecycle contract/SQL pairs,
Finish-job deduplication, concurrent claims and upgrades, retry schedules/budgets,
lease expiry/fencing, source completeness, damaged files, capture intervals and
supplemental sessions, immutable originals, recording during blocked inference,
actual SQLite COMMIT denial, and subprocess exits before/after claim, original,
segment result, and whole-job commits. Repeated recovery preserves successful rows.
These are process-crash tests, not power-loss qualification.

Permanent acceptance regressions now also cover:

- `test_concurrent_finalizers_same_claim_complete_exactly_once`: two independent
  SQLite connections synchronize before finalizing the same valid claim. Exactly
  one completes; the other is fenced out, with one completion event and unchanged
  original segment results.
- `test_stale_worker_cannot_overwrite_new_owner_published_text`: a new owner
  publishes text after expired-claim recovery; the stale owner's replacement is
  rejected, the newer text/producing token stay unchanged, and the new owner can
  finish the complete source set.
- `test_populated_v3_upgrade_transcribes_existing_finalized_recording`: a v3
  database is populated through the existing synthetic recording/Finish path,
  upgraded to v4, and its existing job transcribed once. Recording, interval,
  segment, and job identities survive the upgrade, with source metadata unchanged.

Independent review found two consequential issues and verified the corrections:
concurrent upgrades needed locked reinspection/consistent read snapshots, and
the store needed exact source-set checks plus claim-to-original/source ownership
checks. Targeted regressions cover both. A separate review run passed 80
transcription tests after those fixes; later integration tests extend that coverage.
Final full-suite verification after the accepted regression additions:
**588 passed in 103.79 seconds**. The transcription subset contains 131 passing
tests. The three permanent acceptance regressions also passed as a targeted run
(3 passed in 1.34 seconds). A strengthened diagnostics assertion was previously
rerun independently as a targeted test and passed. No physical microphone, model,
or network was used by these checks.

## Limits and next increment

This is an explicit synchronous host API, not an autonomous service or desktop UI.
There is no entry-wide transcript assembly, sentence stitching, word alignment,
language refinement, search, encryption, retention cleanup, or real ASR. Future
derived versions must reference originals and preserve them, not update these rows.
The first adopted engine/configuration is frozen; reprocessing with another model
requires a separately designed versioned request, not silent replacement.

A future faster-whisper adapter implements the existing engine protocol and supplies
explicit local model/version/configuration provenance. It must consume validated
inputs, classify failures, and provide sufficiently frequent cooperative heartbeats
(or use a separately supervised inference/lease arrangement). A non-cooperative
slow engine loses its lease and its output is discarded; this increment does not
promise to forcibly cancel or bound arbitrary engine calls. No model is selected,
installed, imported, or downloaded here. Recommended next increment: a separately
approved local ASR adapter and manual qualification using an explicitly provided
local model, with no implicit download and no recording-path changes.

Persisted leases use UTC wall time; clock jumps can delay recovery or cause early
expiry. The database is authoritative but cannot transact atomically with external
filesystem changes. Hashes validate observed bytes; external file replacement
between checks/commit or after completion remains a qualification limit, as do the
existing reparse-point/hard-link races and physical storage/power-loss behavior.
No corrupted-database reconstruction is implemented. Large manifests and shared
SQLite/disk contention need performance qualification. The prototype remains
unencrypted and unsuitable for sensitive recordings or transcripts.

The Increment 4A architecture and partial-result semantics are accepted. The final
regression/documentation improvements remain subject to user review before commit.
No commit, push, model download, or start of Increment 4B is authorized here.
