CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64)
) STRICT;

CREATE TABLE journal_entries (
    entry_id TEXT PRIMARY KEY NOT NULL CHECK(length(entry_id) = 36 AND substr(entry_id,9,1) = '-' AND substr(entry_id,14,1) = '-' AND substr(entry_id,19,1) = '-' AND substr(entry_id,24,1) = '-' AND length(replace(entry_id,'-','')) = 32 AND replace(entry_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    display_timezone TEXT NOT NULL CHECK(length(display_timezone) > 0)
) STRICT;

CREATE TABLE recording_sessions (
    session_id TEXT PRIMARY KEY NOT NULL CHECK(length(session_id) = 36 AND substr(session_id,9,1) = '-' AND substr(session_id,14,1) = '-' AND substr(session_id,19,1) = '-' AND substr(session_id,24,1) = '-' AND length(replace(session_id,'-','')) = 32 AND replace(session_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    entry_id TEXT NOT NULL REFERENCES journal_entries(entry_id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    state TEXT NOT NULL CHECK(state IN ('starting','recording','pausing','paused','resuming','finalizing','finished','interrupted','recovery_required')),
    input_revision INTEGER NOT NULL DEFAULT 0 CHECK(input_revision >= 0),
    interruption_code TEXT CHECK(interruption_code IN ('control_timeout','ui_crash','recorder_crash','windows_shutdown','capture_failed','storage_failed')),
    CHECK(state NOT IN ('interrupted','recovery_required') OR interruption_code IS NOT NULL)
) STRICT;

CREATE TRIGGER session_initial_state BEFORE INSERT ON recording_sessions
WHEN NEW.state <> 'starting'
BEGIN SELECT RAISE(ABORT, 'session must start in starting state'); END;

CREATE TRIGGER session_state_transition BEFORE UPDATE OF state ON recording_sessions
WHEN NEW.state <> OLD.state AND NOT (
    (OLD.state = 'starting' AND NEW.state IN ('recording','interrupted')) OR
    (OLD.state = 'recording' AND NEW.state IN ('pausing','finalizing','interrupted')) OR
    (OLD.state = 'pausing' AND NEW.state IN ('paused','interrupted')) OR
    (OLD.state = 'paused' AND NEW.state IN ('resuming','finalizing','interrupted')) OR
    (OLD.state = 'resuming' AND NEW.state IN ('recording','interrupted')) OR
    (OLD.state = 'finalizing' AND NEW.state IN ('finished','interrupted')) OR
    (OLD.state = 'interrupted' AND NEW.state = 'recovery_required') OR
    (OLD.state = 'recovery_required' AND NEW.state IN ('paused','finalizing'))
)
BEGIN SELECT RAISE(ABORT, 'invalid session state transition'); END;

CREATE TABLE capture_intervals (
    interval_id TEXT PRIMARY KEY NOT NULL CHECK(length(interval_id) = 36 AND substr(interval_id,9,1) = '-' AND substr(interval_id,14,1) = '-' AND substr(interval_id,19,1) = '-' AND substr(interval_id,24,1) = '-' AND length(replace(interval_id,'-','')) = 32 AND replace(interval_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    started_at TEXT NOT NULL CHECK(julianday(started_at) IS NOT NULL AND started_at GLOB '*T*Z'),
    ended_at TEXT CHECK(ended_at IS NULL OR (julianday(ended_at) IS NOT NULL AND ended_at GLOB '*T*Z')),
    device_id TEXT NOT NULL CHECK(length(device_id) > 0),
    sample_rate INTEGER NOT NULL CHECK(sample_rate > 0),
    channels INTEGER NOT NULL CHECK(channels = 1),
    sample_width INTEGER NOT NULL CHECK(sample_width = 2),
    end_reason TEXT CHECK(end_reason IN ('pause','finish','control_timeout','ui_crash','recorder_crash','windows_shutdown','capture_failed','storage_failed')),
    CHECK((ended_at IS NULL) = (end_reason IS NULL)),
    UNIQUE(session_id, sequence),
    UNIQUE(interval_id, session_id)
) STRICT;
CREATE UNIQUE INDEX one_open_interval_per_session ON capture_intervals(session_id) WHERE ended_at IS NULL;

CREATE TABLE audio_segments (
    segment_id TEXT PRIMARY KEY NOT NULL CHECK(length(segment_id) = 36 AND substr(segment_id,9,1) = '-' AND substr(segment_id,14,1) = '-' AND substr(segment_id,19,1) = '-' AND substr(segment_id,24,1) = '-' AND length(replace(segment_id,'-','')) = 32 AND replace(segment_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    interval_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    start_frame INTEGER NOT NULL CHECK(start_frame >= 0),
    frame_count INTEGER NOT NULL DEFAULT 0 CHECK(frame_count >= 0),
    byte_length INTEGER NOT NULL DEFAULT 0 CHECK(byte_length >= 0),
    relative_path TEXT NOT NULL UNIQUE CHECK(length(relative_path) > 0 AND relative_path NOT GLOB '/*' AND instr(relative_path, '\') = 0 AND instr(relative_path, ':') = 0 AND instr('/' || relative_path || '/', '/../') = 0 AND instr('/' || relative_path || '/', '/./') = 0 AND instr(relative_path, '//') = 0 AND substr(relative_path,-1) <> '/'),
    state TEXT NOT NULL CHECK(state IN ('writing','finalized','recovered','unresolved')),
    sha256 TEXT CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    recovered_from_segment_id TEXT REFERENCES audio_segments(segment_id) ON DELETE RESTRICT,
    CHECK(recovered_from_segment_id IS NULL OR recovered_from_segment_id <> segment_id),
    CHECK(state NOT IN ('finalized','recovered') OR (sha256 IS NOT NULL AND frame_count > 0 AND byte_length > 0)),
    FOREIGN KEY(interval_id, session_id) REFERENCES capture_intervals(interval_id, session_id) ON DELETE RESTRICT,
    UNIQUE(session_id, sequence)
) STRICT;

CREATE TABLE lifecycle_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK(length(event_id) = 36 AND substr(event_id,9,1) = '-' AND substr(event_id,14,1) = '-' AND substr(event_id,19,1) = '-' AND substr(event_id,24,1) = '-' AND length(replace(event_id,'-','')) = 32 AND replace(event_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    state TEXT NOT NULL CHECK(state IN ('starting','recording','pausing','paused','resuming','finalizing','finished','interrupted','recovery_required')),
    code TEXT NOT NULL CHECK(code IN ('state_changed','command_acknowledged','control_timeout','ui_crash','recorder_crash','windows_shutdown','normal_shutdown','capture_failed','storage_failed','recovery_required')),
    UNIQUE(session_id, sequence)
) STRICT;
CREATE TRIGGER lifecycle_no_update BEFORE UPDATE ON lifecycle_events
BEGIN SELECT RAISE(ABORT, 'lifecycle events are append only'); END;
CREATE TRIGGER lifecycle_no_delete BEFORE DELETE ON lifecycle_events
BEGIN SELECT RAISE(ABORT, 'lifecycle events are append only'); END;
CREATE TRIGGER lifecycle_no_replace BEFORE INSERT ON lifecycle_events
WHEN EXISTS (SELECT 1 FROM lifecycle_events WHERE event_id = NEW.event_id
             OR (session_id = NEW.session_id AND sequence = NEW.sequence))
BEGIN SELECT RAISE(ABORT, 'lifecycle events are append only'); END;

CREATE TABLE processing_jobs (
    job_id TEXT PRIMARY KEY NOT NULL CHECK(length(job_id) = 36 AND substr(job_id,9,1) = '-' AND substr(job_id,14,1) = '-' AND substr(job_id,19,1) = '-' AND substr(job_id,24,1) = '-' AND length(replace(job_id,'-','')) = 32 AND replace(job_id,'-','') NOT GLOB '*[^0-9a-f]*'),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    job_type TEXT NOT NULL CHECK(job_type = 'process_session'),
    input_revision INTEGER NOT NULL CHECK(input_revision >= 0),
    pipeline_version TEXT NOT NULL CHECK(length(pipeline_version) > 0),
    state TEXT NOT NULL CHECK(state IN ('pending','running','succeeded','retryable_failure','blocked')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    lease_owner TEXT,
    lease_expires_at TEXT CHECK(lease_expires_at IS NULL OR (julianday(lease_expires_at) IS NOT NULL AND lease_expires_at GLOB '*T*Z')),
    error_code TEXT CHECK(error_code IS NULL OR (length(error_code) BETWEEN 1 AND 64 AND error_code NOT GLOB '*[^a-z0-9_]*')),
    CHECK((state = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    UNIQUE(session_id, input_revision, job_type, pipeline_version)
) STRICT;
