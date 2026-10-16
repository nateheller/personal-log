-- Original segment identities, paths and hashes remain evidence during recovery.
CREATE TABLE segment_storage (
    segment_id TEXT PRIMARY KEY NOT NULL REFERENCES audio_segments(segment_id) ON DELETE RESTRICT,
    staging_path TEXT NOT NULL UNIQUE,
    accepted_frames INTEGER NOT NULL DEFAULT 0 CHECK(accepted_frames >= 0),
    durable_frames INTEGER NOT NULL DEFAULT 0 CHECK(durable_frames BETWEEN 0 AND accepted_frames),
    publication_sha256 TEXT CHECK(length(publication_sha256) = 64 AND publication_sha256 NOT GLOB '*[^0-9a-f]*'),
    publication_bytes INTEGER CHECK(publication_bytes > 44),
    publication_frames INTEGER CHECK(publication_frames > 0),
    CHECK((publication_sha256 IS NULL) = (publication_bytes IS NULL)),
    CHECK((publication_sha256 IS NULL) = (publication_frames IS NULL)),
    CHECK(publication_frames IS NULL OR publication_frames = accepted_frames)
) STRICT;

CREATE TABLE segment_recoveries (
    segment_id TEXT PRIMARY KEY NOT NULL REFERENCES audio_segments(segment_id) ON DELETE RESTRICT,
    relative_path TEXT NOT NULL UNIQUE,
    source_path TEXT NOT NULL,
    frame_count INTEGER NOT NULL CHECK(frame_count > 0),
    byte_length INTEGER NOT NULL CHECK(byte_length = 44 + frame_count * 2),
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    missing_frames INTEGER NOT NULL CHECK(missing_frames >= 0),
    partial_bytes INTEGER NOT NULL CHECK(partial_bytes BETWEEN 0 AND 1),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z')
) STRICT;
CREATE TRIGGER recovery_no_update BEFORE UPDATE ON segment_recoveries
BEGIN SELECT RAISE(ABORT, 'recovery provenance is immutable'); END;
CREATE TRIGGER recovery_no_delete BEFORE DELETE ON segment_recoveries
BEGIN SELECT RAISE(ABORT, 'recovery provenance is immutable'); END;
CREATE TRIGGER recovery_no_replace BEFORE INSERT ON segment_recoveries
WHEN EXISTS (SELECT 1 FROM segment_recoveries WHERE segment_id = NEW.segment_id OR relative_path = NEW.relative_path)
BEGIN SELECT RAISE(ABORT, 'recovery provenance is immutable'); END;

CREATE TABLE recovery_findings (
    finding_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    segment_id TEXT REFERENCES audio_segments(segment_id) ON DELETE RESTRICT,
    code TEXT NOT NULL,
    severity TEXT NOT NULL CHECK(severity IN ('info','uncertain','gap','error')),
    source_path TEXT NOT NULL,
    missing_frames INTEGER NOT NULL DEFAULT 0 CHECK(missing_frames >= 0),
    partial_bytes INTEGER NOT NULL DEFAULT 0 CHECK(partial_bytes >= 0),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL AND created_at GLOB '*T*Z')
) STRICT;

ALTER TABLE recording_sessions ADD COLUMN recovery_status TEXT NOT NULL DEFAULT 'unchecked'
    CHECK(recovery_status IN ('unchecked','clean','needs_attention'));
