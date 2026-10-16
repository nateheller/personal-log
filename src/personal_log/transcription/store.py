"""SQLite-authoritative queue and immutable originals; no inference or capture."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import sqlite3
from uuid import uuid4

from personal_log.storage.database import open_database
from personal_log.storage.paths import prepare_data_root
from .contracts import JobState, LostClaim, TerminalError, canonical, transition
from .sources import manifest


def utc_now():
    return datetime.now(timezone.utc)


def stamp(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('An aware processing clock is required')
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


@dataclass(frozen=True)
class Claim:
    job_id: str
    token: str
    owner: str
    attempt: int


class TranscriptionStore:
    def __init__(self, *, repository, root=None, clock=utc_now, lease_seconds=60,
                 retry_seconds=30, max_attempts=3, hook=lambda boundary: None):
        if (type(max_attempts) is not int or not 1 <= max_attempts <= 20
                or not isinstance(lease_seconds, (int, float)) or not 0 < lease_seconds <= 3600
                or not isinstance(retry_seconds, (int, float)) or not 0 <= retry_seconds <= 3600):
            raise ValueError('Invalid bounded lease/retry policy')
        self.root = prepare_data_root(repository=repository, root=root)
        self.clock, self.lease_seconds, self.retry_seconds = clock, lease_seconds, retry_seconds
        self.max_attempts, self.hook = max_attempts, hook
        self.db = open_database(repository=repository, root=self.root)
        self.db.row_factory = sqlite3.Row

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self, label, guard=None):
        if self.db.in_transaction:
            raise RuntimeError('Nested processing transaction')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.hook(label + '.before_commit')
            if guard is not None:
                guard()
            self.db.commit()
        except BaseException:
            if self.db.in_transaction:
                self.db.rollback()
            raise
        self.hook(label + '.committed')

    def _event(self, job_id, state, now, token=None, error=None):
        self.db.execute('INSERT INTO transcription_events VALUES (?,?,?,?,?,?)',
                        (str(uuid4()), job_id, token, now, str(state), error))

    def enqueue(self, identity):
        """Materialize existing eligible Finish jobs; never enqueue interrupted audio."""
        now, added = stamp(self.clock()), 0
        with self.transaction('enqueue'):
            rows = self.db.execute("SELECT p.job_id FROM processing_jobs p JOIN recording_sessions s USING(session_id) "
                "WHERE p.state='pending' AND p.job_type='process_session' AND p.pipeline_version='capture-v1' "
                "AND s.state='finished' AND s.recovery_status='clean' AND p.input_revision=s.input_revision "
                "AND NOT EXISTS(SELECT 1 FROM transcription_jobs t WHERE t.job_id=p.job_id) ORDER BY p.created_at,p.job_id").fetchall()
            for row in rows:
                self.db.execute('INSERT INTO transcription_jobs (job_id,engine_json,created_at,updated_at,max_attempts) '
                    'VALUES (?,?,?,?,?)', (row['job_id'], identity.json(), now, now, self.max_attempts))
                self._event(row['job_id'], JobState.PENDING, now)
                added += 1
        return added

    def claim(self, identity, owner):
        if not isinstance(owner, str) or not owner.strip():
            raise ValueError('An explicit processing owner is required')
        now = self.clock()
        with self.transaction('claim'):
            row = self.db.execute("SELECT t.* FROM transcription_jobs t JOIN processing_jobs p USING(job_id) "
                "JOIN recording_sessions s USING(session_id) WHERE t.state IN ('pending','retryable_failure','interrupted') "
                "AND t.attempts<t.max_attempts AND (t.next_attempt_at IS NULL OR t.next_attempt_at<=?) "
                "AND t.engine_json=? AND p.state IN ('pending','retryable_failure') AND s.state='finished' "
                "AND s.recovery_status='clean' AND s.input_revision=p.input_revision ORDER BY t.created_at,t.job_id LIMIT 1",
                (stamp(now), identity.json())).fetchone()
            if row is None:
                return None
            transition(row['state'], JobState.RUNNING)
            claim = Claim(row['job_id'], str(uuid4()), owner, row['attempts'] + 1)
            expires = stamp(now + timedelta(seconds=self.lease_seconds))
            self.db.execute("UPDATE transcription_jobs SET state='running',attempts=?,owner=?,claim_token=?,"
                'lease_expires_at=?,next_attempt_at=NULL,error_code=NULL,updated_at=? WHERE job_id=?',
                (claim.attempt, owner, claim.token, expires, stamp(now), claim.job_id))
            self.db.execute("INSERT INTO transcription_attempts VALUES (?,?,?,?,?,NULL,'running',NULL)",
                            (claim.token, claim.job_id, claim.attempt, owner, stamp(now)))
            self.db.execute("UPDATE processing_jobs SET state='running',attempts=attempts+1,lease_owner=?,"
                'lease_expires_at=?,error_code=NULL WHERE job_id=?', (claim.token, expires, claim.job_id))
            self._event(claim.job_id, JobState.RUNNING, stamp(now), claim.token)
        return claim

    def _owned(self, claim):
        row = self.db.execute('SELECT t.*,p.state AS parent_state,p.lease_owner AS parent_owner FROM '
            'transcription_jobs t JOIN processing_jobs p USING(job_id) WHERE t.job_id=?', (claim.job_id,)).fetchone()
        if (row is None or row['state'] != 'running' or row['claim_token'] != claim.token
                or row['owner'] != claim.owner or row['attempts'] != claim.attempt
                or row['lease_expires_at'] <= stamp(self.clock()) or row['parent_state'] != 'running'
                or row['parent_owner'] != claim.token):
            raise LostClaim('Claim expired, blocked, or no longer owned')
        return row

    def renew(self, claim):
        old_deadline = []
        with self.transaction('renew', lambda: self._deadline(old_deadline[0])):
            old_deadline.append(self._owned(claim)['lease_expires_at'])
            now = self.clock()
            expires = stamp(now + timedelta(seconds=self.lease_seconds))
            self.db.execute('UPDATE transcription_jobs SET lease_expires_at=?,updated_at=? WHERE job_id=?',
                            (expires, stamp(now), claim.job_id))
            self.db.execute('UPDATE processing_jobs SET lease_expires_at=? WHERE job_id=?', (expires, claim.job_id))

    def snapshot(self, claim):
        # Read one consistent snapshot. No write lock while inspecting manifests.
        self.db.execute('BEGIN')
        try:
            self._owned(claim)
            return manifest(self.db, claim.job_id)
        finally:
            self.db.rollback()

    def prepare_original(self, claim, document, sources):
        with self.transaction('original', lambda: self._owned(claim)):
            row = self._owned(claim)
            expected, expected_sources = manifest(self.db, claim.job_id)
            if expected != document or sources != expected_sources:
                raise TerminalError('source_metadata_changed')
            existing = self.db.execute('SELECT * FROM original_transcripts WHERE job_id=?', (claim.job_id,)).fetchone()
            if existing:
                if existing['source_json'] != document or existing['engine_json'] != row['engine_json']:
                    raise TerminalError('source_metadata_changed')
                return existing['transcript_id']
            parent = self.db.execute('SELECT * FROM processing_jobs WHERE job_id=?', (claim.job_id,)).fetchone()
            sid = parent['session_id']
            entry = self.db.execute('SELECT entry_id FROM recording_sessions WHERE session_id=?', (sid,)).fetchone()[0]
            transcript_id = str(uuid4())
            self.db.execute('INSERT INTO original_transcripts (transcript_id,job_id,session_id,entry_id,input_revision,'
                'engine_json,source_json,source_sha256,created_at) VALUES (?,?,?,?,?,?,?,?,?)',
                (transcript_id, claim.job_id, sid, entry, parent['input_revision'], row['engine_json'], document,
                 sha256(document.encode()).hexdigest(), stamp(self.clock())))
            for source in sources:
                self.db.execute('INSERT INTO transcript_sources VALUES (?,?,?,?,?)',
                    (transcript_id, source.segment_id, source.sequence, source.interval_id, canonical(asdict(source))))
        return transcript_id

    def has_result(self, transcript_id, source):
        return self.db.execute('SELECT 1 FROM transcript_segments WHERE transcript_id=? AND segment_id=?',
                               (transcript_id, source.segment_id)).fetchone() is not None

    def start_segment(self, claim, transcript_id, source):
        with self.transaction('segment_start', lambda: self._owned(claim)):
            self._owned(claim)
            self._original_source(claim, transcript_id, source)
            self.db.execute("INSERT INTO transcript_segment_attempts VALUES (?,?,?,?,NULL,'running',NULL)",
                            (transcript_id, source.segment_id, claim.token, stamp(self.clock())))

    def publish(self, claim, transcript_id, source, result, document):
        with self.transaction('segment_result', lambda: self._owned(claim)):
            self._owned(claim)
            self._original_source(claim, transcript_id, source)
            if manifest(self.db, claim.job_id)[0] != document:
                raise TerminalError('source_metadata_changed')
            existing = self.db.execute('SELECT text FROM transcript_segments WHERE transcript_id=? AND segment_id=?',
                                       (transcript_id, source.segment_id)).fetchone()
            if existing is not None:
                if existing['text'] != result.text:
                    raise TerminalError('conflicting_result')
                return  # Exact retry, never overwrite the original.
            now = stamp(self.clock())
            self.db.execute('INSERT INTO transcript_segments (transcript_id,segment_id,claim_token,text,created_at) VALUES (?,?,?,?,?)',
                            (transcript_id, source.segment_id, claim.token, result.text, now))
            updated = self.db.execute("UPDATE transcript_segment_attempts SET state='completed',ended_at=? "
                "WHERE transcript_id=? AND segment_id=? AND claim_token=? AND state='running'",
                (now, transcript_id, source.segment_id, claim.token))
            if updated.rowcount != 1:
                raise TerminalError('missing_segment_attempt')

    def _end(self, row, target, code, now):
        transition(row['state'], target)
        next_time = stamp(now + timedelta(seconds=self.retry_seconds)) if target in {JobState.RETRYABLE, JobState.INTERRUPTED} else None
        token, job_id = row['claim_token'], row['job_id']
        self.db.execute('UPDATE transcription_jobs SET state=?,owner=NULL,claim_token=NULL,lease_expires_at=NULL,'
            'next_attempt_at=?,error_code=?,updated_at=? WHERE job_id=?', (target, next_time, code, stamp(now), job_id))
        self.db.execute('UPDATE transcription_attempts SET state=?,ended_at=?,error_code=? WHERE claim_token=?',
                        (target, stamp(now), code, token))
        # Any in-flight segment did not publish; preserve successful results.
        segment_state = JobState.INTERRUPTED if target == JobState.COMPLETED else target
        self.db.execute("UPDATE transcript_segment_attempts SET state=?,ended_at=?,error_code=? WHERE claim_token=? AND state='running'",
                        (segment_state, stamp(now), code, token))
        parent_state = {JobState.COMPLETED: 'succeeded', JobState.TERMINAL: 'blocked',
                        JobState.RETRYABLE: 'retryable_failure', JobState.INTERRUPTED: 'retryable_failure'}[target]
        self.db.execute("UPDATE processing_jobs SET state=?,lease_owner=NULL,lease_expires_at=NULL,error_code=? "
                        "WHERE job_id=? AND state<>'blocked'", (parent_state, code, job_id))
        self._event(job_id, target, stamp(now), token, code)

    def fail(self, claim, state, code):
        if state not in {JobState.RETRYABLE, JobState.TERMINAL, JobState.INTERRUPTED}:
            raise ValueError('Failure state required')
        deadline = []
        with self.transaction('failure', lambda: self._deadline(deadline[0])):
            row = self._owned(claim)
            deadline.append(row['lease_expires_at'])
            if row['attempts'] >= row['max_attempts'] and state != JobState.TERMINAL:
                state, code = JobState.TERMINAL, 'retry_exhausted'
            self._end(row, state, code, self.clock())

    def complete(self, claim, transcript_id, document):
        deadline = []
        with self.transaction('complete', lambda: self._deadline(deadline[0])):
            row = self._owned(claim)
            deadline.append(row['lease_expires_at'])
            expected, sources = manifest(self.db, claim.job_id)
            if expected != document:
                raise TerminalError('source_metadata_changed')
            header = self.db.execute('SELECT * FROM original_transcripts WHERE transcript_id=? AND job_id=?',
                                     (transcript_id, claim.job_id)).fetchone()
            if header is None or header['source_json'] != document:
                raise TerminalError('invalid_original')
            actual_sources = self.db.execute('SELECT source_json FROM transcript_sources WHERE transcript_id=? ORDER BY sequence',
                                            (transcript_id,)).fetchall()
            results = {r[0] for r in self.db.execute('SELECT segment_id FROM transcript_segments WHERE transcript_id=?', (transcript_id,))}
            if ([r[0] for r in actual_sources] != [canonical(asdict(s)) for s in sources]
                    or results != {s.segment_id for s in sources}):
                raise TerminalError('incomplete_results')
            self._end(row, JobState.COMPLETED, None, self.clock())

    def _deadline(self, expires):
        if expires <= stamp(self.clock()):
            raise LostClaim('Lease expired before commit')

    def _original_source(self, claim, transcript_id, source):
        row = self.db.execute('SELECT s.source_json FROM transcript_sources s JOIN original_transcripts o USING(transcript_id) '
            'WHERE s.transcript_id=? AND s.segment_id=? AND o.job_id=?',
            (transcript_id, source.segment_id, claim.job_id)).fetchone()
        if row is None or row[0] != canonical(asdict(source)):
            raise TerminalError('source_claim_mismatch')

    def recover(self):
        """Only expired or externally blocked claims; never steal a live lease."""
        now, count = self.clock(), 0
        with self.transaction('recover'):
            rows = self.db.execute("SELECT t.*,p.state AS parent_state FROM transcription_jobs t "
                "JOIN processing_jobs p USING(job_id) WHERE t.state='running' AND (t.lease_expires_at<=? OR p.state='blocked')",
                (stamp(now),)).fetchall()
            for row in rows:
                terminal = row['attempts'] >= row['max_attempts'] or row['parent_state'] == 'blocked'
                target = JobState.TERMINAL if terminal else JobState.INTERRUPTED
                code = 'source_blocked' if row['parent_state'] == 'blocked' else 'lease_expired'
                self._end(row, target, code, now)
                count += 1
        return count
