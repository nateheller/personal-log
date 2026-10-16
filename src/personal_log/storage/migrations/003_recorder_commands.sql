CREATE TABLE recorder_commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    controller_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    session_id TEXT NOT NULL REFERENCES recording_sessions(session_id) ON DELETE RESTRICT,
    command_sha256 TEXT NOT NULL CHECK(length(command_sha256) = 64),
    response_json TEXT NOT NULL CHECK(json_valid(response_json)),
    UNIQUE(controller_id, sequence)
) STRICT;
CREATE TRIGGER command_no_update BEFORE UPDATE ON recorder_commands
BEGIN SELECT RAISE(ABORT, 'command receipts are immutable'); END;
CREATE TRIGGER command_no_delete BEFORE DELETE ON recorder_commands
BEGIN SELECT RAISE(ABORT, 'command receipts are immutable'); END;
CREATE TRIGGER command_no_replace BEFORE INSERT ON recorder_commands
WHEN EXISTS (SELECT 1 FROM recorder_commands WHERE command_id=NEW.command_id
    OR (controller_id=NEW.controller_id AND sequence=NEW.sequence))
BEGIN SELECT RAISE(ABORT, 'command receipts are immutable'); END;
