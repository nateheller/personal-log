-- Keep the existing Finish/job contract; this is a one-to-one execution detail.
CREATE TABLE transcription_jobs (
    job_id TEXT PRIMARY KEY NOT NULL REFERENCES processing_jobs(job_id) ON DELETE RESTRICT,
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1),
    engine_json TEXT NOT NULL CHECK(json_valid(engine_json)),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN
        ('pending','running','completed','retryable_failure','terminal_failure','interrupted')),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    updated_at TEXT NOT NULL CHECK(julianday(updated_at) IS NOT NULL AND updated_at GLOB '*T*Z'),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND max_attempts),
    max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 20),
    owner TEXT,
    claim_token TEXT,
    lease_expires_at TEXT CHECK(lease_expires_at IS NULL OR (julianday(lease_expires_at) IS NOT NULL AND lease_expires_at GLOB '*T*Z')),
    next_attempt_at TEXT CHECK(next_attempt_at IS NULL OR (julianday(next_attempt_at) IS NOT NULL AND next_attempt_at GLOB '*T*Z')),
    error_code TEXT CHECK(error_code IS NULL OR (length(error_code) BETWEEN 1 AND 64 AND error_code NOT GLOB '*[^a-z0-9_]*')),
    CHECK((state='running') = (owner IS NOT NULL AND claim_token IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK((owner IS NULL) = (claim_token IS NULL) AND (owner IS NULL) = (lease_expires_at IS NULL)),
    CHECK(state NOT IN ('retryable_failure','terminal_failure','interrupted') OR error_code IS NOT NULL)
) STRICT;
CREATE TRIGGER transcription_initial BEFORE INSERT ON transcription_jobs
WHEN NEW.state<>'pending' OR NEW.attempts<>0
BEGIN SELECT RAISE(ABORT,'transcription must start pending'); END;
CREATE TRIGGER transcription_transition BEFORE UPDATE OF state ON transcription_jobs
WHEN NEW.state<>OLD.state AND NOT (
    (OLD.state IN ('pending','retryable_failure','interrupted') AND NEW.state='running') OR
    (OLD.state='running' AND NEW.state IN ('completed','retryable_failure','terminal_failure','interrupted'))
)
BEGIN SELECT RAISE(ABORT,'invalid transcription transition'); END;
CREATE TRIGGER transcription_identity BEFORE UPDATE ON transcription_jobs
WHEN NEW.job_id<>OLD.job_id OR NEW.engine_json<>OLD.engine_json OR NEW.created_at<>OLD.created_at
    OR NEW.max_attempts<>OLD.max_attempts OR NEW.schema_version<>OLD.schema_version
    OR (NEW.state='running' AND OLD.state<>'running' AND NEW.attempts<>OLD.attempts+1)
    OR (NOT (NEW.state='running' AND OLD.state<>'running') AND NEW.attempts<>OLD.attempts)
    OR (OLD.state='running' AND NEW.state='running' AND
        (NEW.claim_token<>OLD.claim_token OR NEW.owner<>OLD.owner))
BEGIN SELECT RAISE(ABORT,'invalid transcription identity or attempt'); END;

CREATE TABLE transcription_attempts (
    claim_token TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES transcription_jobs(job_id) ON DELETE RESTRICT,
    attempt INTEGER NOT NULL CHECK(attempt>0),
    owner TEXT NOT NULL,
    started_at TEXT NOT NULL CHECK(julianday(started_at) IS NOT NULL AND started_at GLOB '*T*Z'),
    ended_at TEXT CHECK(ended_at IS NULL OR (julianday(ended_at) IS NOT NULL AND ended_at GLOB '*T*Z')),
    state TEXT NOT NULL CHECK(state IN ('running','completed','retryable_failure','terminal_failure','interrupted')),
    error_code TEXT,
    UNIQUE(job_id,attempt),
    CHECK((state='running') = (ended_at IS NULL))
) STRICT;

CREATE TABLE transcription_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES transcription_jobs(job_id) ON DELETE RESTRICT,
    claim_token TEXT REFERENCES transcription_attempts(claim_token) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','running','completed','retryable_failure','terminal_failure','interrupted')),
    error_code TEXT
) STRICT;

CREATE TABLE original_transcripts (
    transcript_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL UNIQUE REFERENCES transcription_jobs(job_id) ON DELETE RESTRICT,
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1),
    kind TEXT NOT NULL DEFAULT 'original' CHECK(kind='original'),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    entry_id TEXT NOT NULL REFERENCES journal_entries(entry_id) ON DELETE RESTRICT,
    input_revision INTEGER NOT NULL CHECK(input_revision>=0),
    engine_json TEXT NOT NULL CHECK(json_valid(engine_json)),
    source_json TEXT NOT NULL CHECK(json_valid(source_json)),
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64 AND source_sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z')
) STRICT;
CREATE TABLE transcript_sources (
    transcript_id TEXT NOT NULL REFERENCES original_transcripts(transcript_id) ON DELETE RESTRICT,
    segment_id TEXT NOT NULL REFERENCES audio_segments(segment_id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK(sequence>=0),
    interval_id TEXT NOT NULL REFERENCES capture_intervals(interval_id) ON DELETE RESTRICT,
    source_json TEXT NOT NULL CHECK(json_valid(source_json)),
    PRIMARY KEY(transcript_id,segment_id),
    UNIQUE(transcript_id,sequence)
) STRICT;
CREATE TABLE transcript_segment_attempts (
    transcript_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    claim_token TEXT NOT NULL REFERENCES transcription_attempts(claim_token) ON DELETE RESTRICT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    state TEXT NOT NULL CHECK(state IN ('running','completed','retryable_failure','terminal_failure','interrupted')),
    error_code TEXT,
    PRIMARY KEY(transcript_id,segment_id,claim_token),
    FOREIGN KEY(transcript_id,segment_id) REFERENCES transcript_sources(transcript_id,segment_id) ON DELETE RESTRICT,
    CHECK((state='running') = (ended_at IS NULL))
) STRICT;
CREATE TABLE transcript_segments (
    transcript_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    claim_token TEXT NOT NULL REFERENCES transcription_attempts(claim_token) ON DELETE RESTRICT,
    text TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'completed' CHECK(state='completed'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z'),
    PRIMARY KEY(transcript_id,segment_id),
    FOREIGN KEY(transcript_id,segment_id) REFERENCES transcript_sources(transcript_id,segment_id) ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER original_no_update BEFORE UPDATE ON original_transcripts BEGIN SELECT RAISE(ABORT,'original transcript is immutable'); END;
CREATE TRIGGER original_no_delete BEFORE DELETE ON original_transcripts BEGIN SELECT RAISE(ABORT,'original transcript is immutable'); END;
CREATE TRIGGER original_no_replace BEFORE INSERT ON original_transcripts
WHEN EXISTS(SELECT 1 FROM original_transcripts WHERE transcript_id=NEW.transcript_id OR job_id=NEW.job_id)
BEGIN SELECT RAISE(ABORT,'original transcript is immutable'); END;
CREATE TRIGGER sources_no_update BEFORE UPDATE ON transcript_sources BEGIN SELECT RAISE(ABORT,'source provenance is immutable'); END;
CREATE TRIGGER sources_no_delete BEFORE DELETE ON transcript_sources BEGIN SELECT RAISE(ABORT,'source provenance is immutable'); END;
CREATE TRIGGER sources_no_replace BEFORE INSERT ON transcript_sources
WHEN EXISTS(SELECT 1 FROM transcript_sources WHERE transcript_id=NEW.transcript_id AND (segment_id=NEW.segment_id OR sequence=NEW.sequence))
BEGIN SELECT RAISE(ABORT,'source provenance is immutable'); END;
CREATE TRIGGER text_no_update BEFORE UPDATE ON transcript_segments BEGIN SELECT RAISE(ABORT,'original text is immutable'); END;
CREATE TRIGGER text_no_delete BEFORE DELETE ON transcript_segments BEGIN SELECT RAISE(ABORT,'original text is immutable'); END;
CREATE TRIGGER text_no_replace BEFORE INSERT ON transcript_segments
WHEN EXISTS(SELECT 1 FROM transcript_segments WHERE transcript_id=NEW.transcript_id AND segment_id=NEW.segment_id)
BEGIN SELECT RAISE(ABORT,'original text is immutable'); END;
CREATE TRIGGER transcription_events_no_update BEFORE UPDATE ON transcription_events BEGIN SELECT RAISE(ABORT,'events are immutable'); END;
CREATE TRIGGER transcription_events_no_delete BEFORE DELETE ON transcription_events BEGIN SELECT RAISE(ABORT,'events are immutable'); END;
CREATE TRIGGER transcription_events_no_replace BEFORE INSERT ON transcription_events
WHEN EXISTS(SELECT 1 FROM transcription_events WHERE event_id=NEW.event_id)
BEGIN SELECT RAISE(ABORT,'events are immutable'); END;
CREATE TRIGGER attempts_closed BEFORE UPDATE ON transcription_attempts
WHEN OLD.state<>'running' OR NEW.state='running' OR NEW.claim_token<>OLD.claim_token
    OR NEW.job_id<>OLD.job_id OR NEW.attempt<>OLD.attempt OR NEW.owner<>OLD.owner OR NEW.started_at<>OLD.started_at
BEGIN SELECT RAISE(ABORT,'closed attempt is immutable'); END;
CREATE TRIGGER attempts_no_delete BEFORE DELETE ON transcription_attempts BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
CREATE TRIGGER attempts_no_replace BEFORE INSERT ON transcription_attempts
WHEN EXISTS(SELECT 1 FROM transcription_attempts WHERE claim_token=NEW.claim_token OR (job_id=NEW.job_id AND attempt=NEW.attempt))
BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
CREATE TRIGGER segment_attempts_closed BEFORE UPDATE ON transcript_segment_attempts
WHEN OLD.state<>'running' OR NEW.state='running' OR NEW.transcript_id<>OLD.transcript_id
    OR NEW.segment_id<>OLD.segment_id OR NEW.claim_token<>OLD.claim_token OR NEW.started_at<>OLD.started_at
BEGIN SELECT RAISE(ABORT,'closed segment attempt is immutable'); END;
CREATE TRIGGER segment_attempts_no_delete BEFORE DELETE ON transcript_segment_attempts BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
CREATE TRIGGER segment_attempts_no_replace BEFORE INSERT ON transcript_segment_attempts
WHEN EXISTS(SELECT 1 FROM transcript_segment_attempts WHERE transcript_id=NEW.transcript_id AND segment_id=NEW.segment_id AND claim_token=NEW.claim_token)
BEGIN SELECT RAISE(ABORT,'attempt history is immutable'); END;
