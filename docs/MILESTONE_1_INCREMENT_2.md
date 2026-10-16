# Segment persistence and crash recovery

**WARNING: Audio and metadata are UNENCRYPTED. Use disposable synthetic test data
only. Do not record personal or sensitive content.**

## Scope and usage

This increment adds synchronous storage worker code, not a real-time capture loop,
recorder process, watchdog, UI, transcription, encryption, or retention. Existing
themes and recorder boundary contracts are unchanged. A future dedicated recorder
process must feed storage off the microphone callback and enforce control leases.

`SegmentStore(repository=checkout, root=external_root)` is a context manager that
owns an external data-root lock and its SQLite connection. `begin_session()` creates
disposable entry/session/interval metadata. Passing an existing entry ID creates a
supplemental session. `writer(session_id, interval_id)` accepts `AudioBlock` values
from the fake capture interface; start_frame is relative to its capture interval.
`append(block)` validates continuity and persists PCM. `checkpoint()` flushes a tail;
`finish()` finalizes audio and atomically closes the interval, changes session state,
and appends lifecycle events. It returns ordered segment references only after the
required steps complete. Job execution and Finish/enqueue integration remain later
work; this increment does not execute or create transcription jobs.

Closing/abandoning a writer without Finish closes its descriptor without claiming
success. Any persistence failure closes it, attempts to mark interruption, and
raises `PersistenceError`. If even that metadata write fails, startup recovery
recognizes the abandoned session state. A caller must not treat an exception as a
save acknowledgment, even if some or all durable artifacts already exist.

After opening the store, call `recover()` before creating a new recording if there
may be abandoned sessions. It returns persistent, evidence-linked findings and
does not start capture or automatically finish interrupted sessions. Interrupted
sessions remain `recovery_required`. A finished session's capture history stays
finished if later file damage is detected; its separate recovery_status becomes
`needs_attention`, and unfinished processing jobs are blocked.

## Persistence order

The mono PCM16 format and capture interval's sample rate determine segment size:
exactly sample_rate times 20 frames, with a shorter final segment. Blocks are split
at segment and checkpoint boundaries; each accepted frame appears once in segment
order. Memory usage is bounded by input blocks and 64 KiB copying buffers rather
than session length. The caller remains responsible for bounding input block size.

1. Commit a unique segment ID, sequence, start frame, intended WAV path, and staging
   path to SQLite before attempting exclusive staging-file creation.
2. Commit accepted-frame intent before each write. This conservatively records the
   amount that might be missing if a partial write or crash interrupts the call.
   Positive short writes are completed; a zero write or exception stops recording.
3. At most one second of accepted audio is written between checkpoints. Flush the
   unbuffered PCM descriptor with os.fsync, then commit durable_frames. Explicit
   checkpoint handles a short idle tail; there is no wall-clock scheduler yet.
4. Flush/close staging, then build a canonical RIFF/WAV in a uniquely created
   temporary file. Validate format, exact frame count, byte length, and SHA-256
   against the source bytes while streaming. Flush it before publication.
5. Commit the expected publication hash, bytes, and frames. This is an intent, not
   finalized metadata or proof the target exists.
6. Publish in the same directory without replacing any target, then flush the
   published file and the directory where supported. Windows uses MoveFileExW
   WRITE_THROUGH without REPLACE_EXISTING or cross-volume copying. POSIX uses an
   exclusive hard-link operation and removes only the builder's temporary link.
7. Only then commit the segment's finalized state, frame count, size, and hash.

Files reside under the validated external root's `audio/` directory. All original
PCM staging is retained in this prototype, including successful recordings, to
support tests and non-destructive recovery. Failed WAV builders are preserved too.
No audio cleanup or retention policy runs here. New filenames use UUIDs; recovery
targets use the source UUID plus `.recovered.wav`. Existing targets are never opened
for replacement. Directory aliases, redirected file paths, traversal, and checkout
overlap are rejected. The same root lock excludes writer and recovery owners and
is released by the OS on process termination; an active in-process writer also
prevents recovery. Direct low-level database use must respect this ownership rule.

SQLite and the filesystem cannot commit together. Publication before the final
database commit intentionally permits a published orphan after a crash. The
pre-publication hash distinguishes it from unrelated or conflicting files.

## Recovery and provenance

Numbered migration 002 adds segment_storage (accepted/durable/publication intent),
immutable segment_recoveries, recovery_findings, and a session recovery_status.
Migration 001 is unchanged. The runner verifies all applied checksums and commits
each numbered migration with its version. A failed upgrade preserves the previous
complete schema. The database schema is now v2; existing entity and IPC versions
remain v1 because their contracts have not changed.

Recovery runs exclusively and in deterministic session/segment order:

- Atomically record abandoned-session state transitions and events, close abandoned
  intervals, and record an unknown capture-tail finding. Bytes never handed to
  storage before termination cannot have a measured loss count.
- Check finalized artifacts, including finished sessions, against stored hashes,
  lengths, formats, and frame counts. Do not update a hash to bless changed bytes.
- Adopt a published orphan only when it exactly matches its committed publication
  intent; flush it again before committing finalized metadata.
- Otherwise read staging's complete PCM frames, bounded by accepted-frame intent.
  Preserve incomplete trailing bytes and report known missing frames, excess bytes,
  ordering gaps/overlaps, invalid publications, and orphan artifacts. A full staging
  copy that conflicts with a committed audio hash is not accepted as a repair.
- Publish salvage to a distinct deterministic path. Store the result, source path,
  missing-frame count, partial-byte count, hash, and timestamp in an immutable
  recovery row keyed by the original segment ID. Preserve the original segment's
  path/hash/frame metadata, marking its source unresolved. Effective segment queries
  join the repair without adding another logical sequence number.
- If recovery crashes after publication but before its commit, verify and adopt
  that same deterministic repair on retry. Conflicting targets remain untouched and
  unresolved. Repeated successful runs do not add segments, findings, files, or events.
  Deterministic repair paths belong to their source segment intent, so they are not
  classified as unrelated orphans while pending. If an older run recorded an orphan
  finding for a subsequently adopted repair, the immutable recovery record resolves
  that warning: the historical row remains, but recover() no longer returns it.
- Validate existing repairs too. If damaged, preserve their evidence, report the
  problem, and exclude them from effective references; never replace them silently.
  segments() also checks current format, frame count, byte length, and hash on each
  lookup, excluding missing, unreadable, redirected, or damaged files even before
  another recovery pass. Lookup is read-only; recovery persists the invalid finding.
  This requires reading/hashing each candidate and is not a playback-time guarantee
  against an external change after lookup.

Finding IDs derive deterministically from session, segment, code, and path. Findings
are conservative historical evidence, not automatically cleared if files are later
changed externally. Unrelated artifacts with no trustworthy metadata are preserved
and reported, not attached to an invented journal entry. Recovery does not promise
to reconstruct a lost/corrupt SQLite database or authenticate unknown orphan PCM.

## Tests and qualification limits

Run `.venv/Scripts/python.exe -m pytest -q`. Every audio file and database used by
tests is synthetic and created in an external temporary directory. Tests exercise
frame continuity and ordering, periodic checkpoints, positive/zero/partial writes,
flush and finalization errors, publication collisions, actual SQLite COMMIT denial,
transaction/trigger failure, v1 upgrade rollback, corruption/missing files, immutable
repair provenance, and repeated recovery. Child processes deliberately exit at
recording and recovery persistence boundaries, including repeated interruption
before recovery metadata commit. These are process-crash tests, not power cuts.

Windows tests exercise the native publication and OS-lock paths. They do not prove
survival of a real power interruption. os.fsync maps to the Windows CRT _commit;
MoveFileExW WRITE_THROUGH is requested, but same-volume directory-entry durability,
new data-root ancestor creation, storage-controller caches, and hardware honoring
flushes need separate Windows/VM power-loss qualification. Durable checkpoint
metadata is not a universal guarantee against dishonest or failed storage hardware.
The source PCM and recovery findings provide fallback evidence; no fixed loss bound
is advertised. The prototype is not ready for sensitive journals.

Existing filesystem limitations remain: adversarial path replacement between checks
and opens, hard links, other reparse-point types, arbitrary cloud-sync roots and
mapped-drive detection are not comprehensively protected. There is no disk-space
warning monitor; actual disk-full/permission/I/O failures stop and preserve data.
No capture scheduling, UI responsiveness, OS shutdown handling, or real microphone
behavior is qualified by this increment.

Implementation references:
[Python fsync semantics](https://docs.python.org/3/library/os.html#os.fsync) and
[Microsoft MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw).
