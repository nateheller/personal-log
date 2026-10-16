from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import product
from pathlib import Path
import json
import multiprocessing as mp
import os
import sqlite3
import threading

import pytest

from personal_log.recording.fake import AudioFormat, FakeMicrophone
from personal_log.storage import database
from personal_log.storage.segments import SegmentStore
from personal_log.transcription.contracts import (
    EngineIdentity, JobState, LostClaim, SegmentText, TerminalError, TRANSITIONS, transition,
)
from personal_log.transcription.fake import FakeTranscriber
from personal_log.transcription.sources import manifest, validate_file
from personal_log.transcription.store import TranscriptionStore
from personal_log.transcription.worker import TranscriptionWorker


class Clock:
    def __init__(self):
        self.now = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds=61):
        self.now += timedelta(seconds=seconds)


def recording(checkout, root, *, finish=True, frames=43, supplemental=None):
    with SegmentStore(repository=checkout, root=root) as store:
        sid, interval = store.begin_session(audio_format=AudioFormat(sample_rate=1), entry_id=supplemental)
        source = FakeMicrophone(AudioFormat(sample_rate=1))
        source.start()
        writer = store.writer(sid, interval)
        for _ in range(frames):
            writer.append(source.read(1))
        if finish:
            writer.finish(enqueue=True)
        return sid


@pytest.fixture
def setup(external, checkout):
    root = external / 'data'
    sid = recording(checkout, root)
    clock = Clock()
    with TranscriptionStore(repository=checkout, root=root, clock=clock, retry_seconds=0) as store:
        yield store, sid, clock


def state(store):
    return store.db.execute('SELECT state FROM transcription_jobs').fetchone()[0]


def texts(store):
    return store.db.execute('SELECT * FROM transcript_segments ORDER BY segment_id').fetchall()


def test_finish_job_materializes_exactly_once_and_completes(setup):
    store, sid, _ = setup
    engine = FakeTranscriber()
    assert store.enqueue(engine.identity) == 1
    assert store.enqueue(engine.identity) == 0
    assert TranscriptionWorker(store, engine).run_one() == JobState.COMPLETED
    assert len(engine.calls) == 3
    assert TranscriptionWorker(store, engine).run_one() is None
    assert len(texts(store)) == 3
    assert store.db.execute('SELECT count(*) FROM original_transcripts').fetchone()[0] == 1
    assert store.db.execute('SELECT state,attempts FROM processing_jobs').fetchone()[:] == ('succeeded', 1)
    assert store.db.execute('SELECT state FROM recording_sessions WHERE session_id=?', (sid,)).fetchone()[0] == 'finished'


def test_provenance_order_formats_and_source_hashes(setup):
    store, sid, _ = setup
    engine = FakeTranscriber()
    TranscriptionWorker(store, engine).run_one()
    header = store.db.execute('SELECT * FROM original_transcripts').fetchone()
    assert header['session_id'] == sid and header['engine_json'] == engine.identity.json()
    document = json.loads(header['source_json'])
    sources = document['segments']
    assert [s['sequence'] for s in sources] == [0, 1, 2]
    assert [s['frame_count'] for s in sources] == [20, 20, 3]
    assert [s['start_frame'] for s in sources] == [0, 20, 40]
    assert all(s['sample_rate'] == 1 and s['channels'] == 1 and s['sample_width'] == 2 for s in sources)
    assert engine.calls == [s['segment_id'] for s in sources]
    for source in sources:
        result = store.db.execute('SELECT text FROM transcript_segments WHERE segment_id=?', (source['segment_id'],)).fetchone()[0]
        assert source['sha256'] in result
    assert document['entry_id'] == header['entry_id']
    assert header['kind'] == 'original' and header['created_at']


def test_interrupted_recording_not_enqueued_even_if_parent_job_exists(external, checkout):
    root = external / 'data'
    sid = recording(checkout, root, finish=False)
    with SegmentStore(repository=checkout, root=root) as audio:
        with audio.transaction('synthetic_legacy_job'):
            audio.enqueue(sid)
    with TranscriptionStore(repository=checkout, root=root) as store:
        assert store.enqueue(FakeTranscriber.identity) == 0
        assert store.claim(FakeTranscriber.identity, 'worker') is None


def test_concurrent_claims_have_one_owner(setup, checkout):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)

    def attempt(n):
        with TranscriptionStore(repository=checkout, root=store.root) as other:
            return other.claim(FakeTranscriber.identity, f'worker-{n}')

    with ThreadPoolExecutor(max_workers=6) as pool:
        claims = list(pool.map(attempt, range(6)))
    assert len([c for c in claims if c]) == 1
    assert store.db.execute('SELECT count(*) FROM transcription_attempts').fetchone()[0] == 1


def test_concurrent_finalizers_same_claim_complete_exactly_once(setup, checkout):
    store, _, clock = setup
    store.enqueue(FakeTranscriber.identity)
    claim = store.claim(FakeTranscriber.identity, 'owner')
    document, sources = store.snapshot(claim)
    original = store.prepare_original(claim, document, sources)
    for source in sources:
        validate_file(store.root, source)
        store.start_segment(claim, original, source)
        store.publish(claim, original, source, SegmentText(f'synthetic segment {source.sequence}'), document)
    before = [tuple(row) for row in texts(store)]
    barrier = threading.Barrier(2)

    def finalize(_):
        # Separate connections contend on the same valid token, not two claims.
        with TranscriptionStore(repository=checkout, root=store.root, clock=clock) as other:
            barrier.wait(5)
            try:
                other.complete(claim, original, document)
                return 'completed'
            except LostClaim:
                return 'lost_claim'

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(finalize, range(2))) == ['completed', 'lost_claim']
    assert state(store) == 'completed'
    assert store.db.execute('SELECT state FROM processing_jobs').fetchone()[0] == 'succeeded'
    assert store.db.execute("SELECT count(*) FROM transcription_events WHERE state='completed'").fetchone()[0] == 1
    assert store.db.execute("SELECT count(*) FROM transcription_attempts WHERE state='completed'").fetchone()[0] == 1
    assert store.db.execute('SELECT count(*) FROM original_transcripts').fetchone()[0] == 1
    assert [tuple(row) for row in texts(store)] == before


@pytest.mark.parametrize('current,target', list(product(JobState, repeat=2)))
def test_explicit_contract_state_transitions(current, target):
    if target in TRANSITIONS[current]:
        assert transition(current, target) == target
    else:
        with pytest.raises(ValueError):
            transition(current, target)


def test_sql_rejects_invalid_transitions_and_claim_mutation(setup):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute("UPDATE transcription_jobs SET state='completed'")
    claim = store.claim(FakeTranscriber.identity, 'first')
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute("UPDATE transcription_jobs SET claim_token='replacement'")
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute("UPDATE transcription_jobs SET state='pending',owner=NULL,claim_token=NULL,lease_expires_at=NULL")
    store.fail(claim, JobState.TERMINAL, 'engine_terminal')
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute("UPDATE transcription_jobs SET state='pending'")


def test_retry_reuses_successful_segments_without_overwrite(setup):
    store, _, _ = setup
    engine = FakeTranscriber(failures={1: ['retry']})
    worker = TranscriptionWorker(store, engine)
    assert worker.run_one() == JobState.RETRYABLE
    original = [tuple(r) for r in texts(store)]
    assert len(original) == 1
    assert worker.run_one() == JobState.COMPLETED
    assert all(row in [tuple(r) for r in texts(store)] for row in original)
    assert engine.calls.count(engine.calls[0]) == 1
    assert store.db.execute('SELECT state FROM transcription_attempts ORDER BY attempt').fetchall()[0][0] == 'retryable_failure'


@pytest.mark.parametrize('action,target', [('terminal', JobState.TERMINAL), ('interrupt', JobState.INTERRUPTED)])
def test_classified_engine_failures(setup, action, target):
    store, _, _ = setup
    engine = FakeTranscriber(failures={0: [action]})
    assert TranscriptionWorker(store, engine).run_one() == target
    assert len(texts(store)) == 0
    assert store.db.execute('SELECT error_code FROM transcription_jobs').fetchone()[0]
    if target == JobState.INTERRUPTED:
        assert TranscriptionWorker(store, engine).run_one() == JobState.COMPLETED
    else:
        assert TranscriptionWorker(store, engine).run_one() is None


def test_retry_budget_and_persisted_delay(external, checkout):
    root = external / 'data'
    recording(checkout, root)
    clock = Clock()
    with TranscriptionStore(repository=checkout, root=root, clock=clock, retry_seconds=10, max_attempts=2) as store:
        worker = TranscriptionWorker(store, FakeTranscriber(failures={0: ['retry', 'retry']}))
        assert worker.run_one() == JobState.RETRYABLE
        assert worker.run_one() is None
        clock.advance(10)
        assert worker.run_one() == JobState.TERMINAL
        assert store.db.execute('SELECT attempts,error_code FROM transcription_jobs').fetchone()[:] == (2, 'retry_exhausted')
        clock.advance(100)
        assert worker.run_one() is None


def test_expired_claim_recovery_fences_late_result(setup):
    store, _, clock = setup
    store.enqueue(FakeTranscriber.identity)
    old = store.claim(FakeTranscriber.identity, 'old')
    document, sources = store.snapshot(old)
    transcript = store.prepare_original(old, document, sources)
    store.start_segment(old, transcript, sources[0])
    assert store.recover() == 0
    clock.advance()
    with pytest.raises(LostClaim):
        store.renew(old)
    assert store.recover() == 1
    assert store.recover() == 0
    new = store.claim(FakeTranscriber.identity, 'new')
    assert new.token != old.token
    with pytest.raises(LostClaim):
        store.publish(old, transcript, sources[0], SegmentText('late result'), document)
    assert not texts(store)


def test_stale_worker_cannot_overwrite_new_owner_published_text(setup):
    store, _, clock = setup
    store.enqueue(FakeTranscriber.identity)
    old = store.claim(FakeTranscriber.identity, 'old')
    document, sources = store.snapshot(old)
    original = store.prepare_original(old, document, sources)
    store.start_segment(old, original, sources[0])
    clock.advance()
    assert store.recover() == 1
    new = store.claim(FakeTranscriber.identity, 'new')
    assert new.token != old.token
    assert store.prepare_original(new, document, sources) == original
    store.start_segment(new, original, sources[0])
    store.publish(new, original, sources[0], SegmentText('synthetic newer owner text'), document)
    before = [tuple(row) for row in texts(store)]
    with pytest.raises(LostClaim):
        store.publish(old, original, sources[0], SegmentText('synthetic stale replacement'), document)
    assert [tuple(row) for row in texts(store)] == before
    published = texts(store)[0]
    assert published['text'] == 'synthetic newer owner text'
    assert published['claim_token'] == new.token
    # The stale attempt must not interfere with the new owner's finalization.
    for source in sources[1:]:
        validate_file(store.root, source)
        store.start_segment(new, original, source)
        store.publish(new, original, source, SegmentText(f'synthetic segment {source.sequence}'), document)
    store.complete(new, original, document)
    assert state(store) == 'completed'
    assert tuple(store.db.execute('SELECT * FROM transcript_segments WHERE segment_id=?',
                                  (sources[0].segment_id,)).fetchone()) == before[0]


@pytest.mark.parametrize('damage', ['missing', 'mutated', 'truncated', 'partial_set'])
def test_invalid_wav_cannot_complete_with_usable_subset(setup, damage):
    store, _, _ = setup
    rows = store.db.execute('SELECT relative_path FROM audio_segments ORDER BY sequence').fetchall()
    target = store.root / rows[1][0]
    if damage in {'missing', 'partial_set'}:
        target.unlink()  # Only synthetic external fixtures.
    else:
        data = target.read_bytes()
        target.write_bytes(data[:-1] if damage == 'truncated' else data[:-1] + bytes([data[-1] ^ 1]))
    worker = TranscriptionWorker(store, FakeTranscriber())
    assert worker.run_one() == JobState.TERMINAL
    assert store.db.execute('SELECT state FROM processing_jobs').fetchone()[0] == 'blocked'
    assert not texts(store)


def test_audio_changed_during_engine_run_is_not_published(setup):
    store, _, _ = setup

    class MutatingEngine(FakeTranscriber):
        def transcribe(self, audio, source, heartbeat):
            result = super().transcribe(audio, source, heartbeat)
            path = store.root / source.relative_path
            data = path.read_bytes()
            path.write_bytes(data[:-1])
            return result

    assert TranscriptionWorker(store, MutatingEngine()).run_one() == JobState.TERMINAL
    assert not texts(store)


def test_final_rescan_includes_already_successful_inputs(setup):
    store, _, _ = setup
    engine = FakeTranscriber(failures={2: ['retry']})
    worker = TranscriptionWorker(store, engine)
    assert worker.run_one() == JobState.RETRYABLE
    original = [tuple(r) for r in texts(store)]
    (store.root / store.db.execute('SELECT relative_path FROM audio_segments WHERE sequence=0').fetchone()[0]).unlink()
    assert worker.run_one() == JobState.TERMINAL
    assert [tuple(r) for r in texts(store)] == original


@pytest.mark.parametrize('table', ['original_transcripts', 'transcript_sources', 'transcript_segments'])
def test_originals_reject_update_delete_and_replace(setup, table):
    store, _, _ = setup
    TranscriptionWorker(store, FakeTranscriber()).run_one()
    before = [tuple(r) for r in store.db.execute('SELECT * FROM ' + table)]
    first_column = store.db.execute('PRAGMA table_info(' + table + ')').fetchone()[1]
    for sql in ['UPDATE ' + table + ' SET ' + first_column + '=' + first_column,
                'DELETE FROM ' + table, 'INSERT OR REPLACE INTO ' + table + ' SELECT * FROM ' + table]:
        with pytest.raises(sqlite3.IntegrityError):
            store.db.execute(sql)
    assert [tuple(r) for r in store.db.execute('SELECT * FROM ' + table)] == before


def test_result_transaction_failure_rolls_back_result_and_status(setup):
    store, _, clock = setup
    store.db.execute("CREATE TRIGGER reject_result BEFORE UPDATE OF state ON transcript_segment_attempts "
        "WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'synthetic commit failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        TranscriptionWorker(store, FakeTranscriber()).run_one()
    assert not texts(store)
    assert state(store) == 'running'
    assert store.db.execute('SELECT state FROM transcript_segment_attempts').fetchone()[0] == 'running'
    store.db.execute('DROP TRIGGER reject_result')
    clock.advance()
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() == JobState.COMPLETED


def test_incomplete_results_never_complete(setup):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    claim = store.claim(FakeTranscriber.identity, 'worker')
    document, sources = store.snapshot(claim)
    transcript = store.prepare_original(claim, document, sources)
    with pytest.raises(TerminalError, match='incomplete_results'):
        store.complete(claim, transcript, document)
    assert state(store) == 'running'


def _crash_worker(repository, root, boundary):
    def hook(label):
        if label == boundary:
            os._exit(82)

    with TranscriptionStore(repository=Path(repository), root=Path(root), clock=Clock(), retry_seconds=0, hook=hook) as store:
        TranscriptionWorker(store, FakeTranscriber()).run_one()


@pytest.mark.parametrize('boundary', ['claim.committed', 'original.before_commit', 'original.committed',
    'segment_start.committed', 'segment_result.before_commit', 'segment_result.committed',
    'complete.before_commit', 'complete.committed'])
def test_process_crash_recovery_preserves_results_and_identity(external, checkout, boundary):
    root = external / 'data'
    recording(checkout, root)
    child = mp.get_context('spawn').Process(target=_crash_worker, args=(str(checkout), str(root), boundary))
    child.start()
    child.join(15)
    if child.is_alive():
        child.terminate()
        child.join(2)
        pytest.fail('Synthetic worker failed to exit')
    assert child.exitcode == 82
    clock = Clock()
    clock.advance()
    with TranscriptionStore(repository=checkout, root=root, clock=clock, retry_seconds=0) as store:
        original = [tuple(r) for r in texts(store)]
        result = TranscriptionWorker(store, FakeTranscriber()).run_one()
        assert result in {JobState.COMPLETED, None}
        assert state(store) == 'completed'
        assert len(texts(store)) == 3
        assert all(r in [tuple(row) for row in texts(store)] for r in original)
        assert store.db.execute('SELECT count(*) FROM original_transcripts').fetchone()[0] == 1
        assert store.recover() == 0


def test_forward_v3_upgrade_failure_preserves_v3(external, checkout, monkeypatch):
    root = external / 'data'
    root.mkdir()
    db = sqlite3.connect(root / 'recording.sqlite3', isolation_level=None)
    from hashlib import sha256
    for number in (1, 2, 3):
        sql = database._migration_sql(number)
        digest = sha256(sql.encode()).hexdigest()
        db.executescript('BEGIN;' + sql + f"INSERT INTO schema_migrations VALUES({number},'{digest}'); PRAGMA user_version={number}; COMMIT;")
    original = database._migration_sql
    monkeypatch.setattr(database, '_migration_sql', lambda number=1: original(number) + (';INVALID SQL;' if number == 4 else ''))
    with pytest.raises(sqlite3.Error):
        database.migrate(db)
    assert db.execute('PRAGMA user_version').fetchone()[0] == 3
    assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='transcription_jobs'").fetchone()
    monkeypatch.setattr(database, '_migration_sql', original)
    database.migrate(db)
    assert db.execute('PRAGMA user_version').fetchone()[0] == 4
    db.close()


def test_populated_v3_upgrade_transcribes_existing_finalized_recording(external, checkout, monkeypatch):
    root = external / 'data'
    # Use the authoritative recording path with only historical migrations.
    # Restore the current version before opening the transcription store.
    with monkeypatch.context() as legacy:
        legacy.setattr(database, 'SCHEMA_VERSION', 3)
        sid = recording(checkout, root)
    db = sqlite3.connect(root / 'recording.sqlite3')
    try:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 3
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='transcription_jobs'").fetchone()
        job = db.execute('SELECT * FROM processing_jobs').fetchone()
        sessions = db.execute('SELECT * FROM recording_sessions').fetchall()
        intervals = db.execute('SELECT * FROM capture_intervals ORDER BY sequence').fetchall()
        segments = db.execute('SELECT * FROM audio_segments ORDER BY sequence').fetchall()
    finally:
        db.close()
    with TranscriptionStore(repository=checkout, root=root) as store:
        assert store.db.execute('PRAGMA user_version').fetchone()[0] == 4
        assert tuple(store.db.execute('SELECT * FROM processing_jobs').fetchone()) == job
        for table, expected in [('recording_sessions', sessions), ('capture_intervals', intervals), ('audio_segments', segments)]:
            order = '' if table == 'recording_sessions' else ' ORDER BY sequence'
            assert [tuple(row) for row in store.db.execute('SELECT * FROM ' + table + order)] == expected
        engine = FakeTranscriber()
        worker = TranscriptionWorker(store, engine)
        assert worker.run_one() == JobState.COMPLETED
        assert worker.run_one() is None
        assert engine.calls == [row[0] for row in segments]
        header = store.db.execute('SELECT * FROM original_transcripts').fetchone()
        assert header['session_id'] == sid and header['job_id'] == job[0]
        assert len(texts(store)) == len(segments) == 3
        assert store.db.execute('SELECT count(*) FROM original_transcripts').fetchone()[0] == 1
        assert store.db.execute('SELECT job_id,state FROM processing_jobs').fetchone()[:] == (job[0], 'succeeded')
        assert [tuple(row) for row in store.db.execute('SELECT * FROM audio_segments ORDER BY sequence')] == segments


def test_simultaneous_v3_open_and_upgrade_is_serialized(external, checkout, monkeypatch):
    root = external / 'data'
    root.mkdir()
    db = sqlite3.connect(root / 'recording.sqlite3', isolation_level=None)
    from hashlib import sha256
    db.execute('PRAGMA journal_mode=WAL')
    for number in (1, 2, 3):
        sql = database._migration_sql(number)
        digest = sha256(sql.encode()).hexdigest()
        db.executescript('BEGIN;' + sql + f"INSERT INTO schema_migrations VALUES({number},'{digest}'); PRAGMA user_version={number}; COMMIT;")
    db.close()
    barrier, local = threading.Barrier(2), threading.local()
    original = database._migration_sql

    def concurrent_sql(number=1):
        if number == 4 and not getattr(local, 'waited', False):
            local.waited = True
            barrier.wait(5)
        return original(number)

    monkeypatch.setattr(database, '_migration_sql', concurrent_sql)

    def open_one(_):
        db = database.open_database(repository=checkout, root=root)
        try:
            return db.execute('PRAGMA user_version').fetchone()[0]
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(open_one, range(2))) == [4, 4]


def test_truncated_source_list_cannot_create_incomplete_original(setup):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    claim = store.claim(FakeTranscriber.identity, 'worker')
    document, sources = store.snapshot(claim)
    with pytest.raises(TerminalError, match='source_metadata_changed'):
        store.prepare_original(claim, document, sources[:-1])
    assert store.db.execute('SELECT count(*) FROM original_transcripts').fetchone()[0] == 0


def test_cross_job_segment_claim_cannot_publish(setup, checkout):
    store, _, _ = setup
    recording(checkout, store.root)
    store.enqueue(FakeTranscriber.identity)
    first = store.claim(FakeTranscriber.identity, 'first')
    second = store.claim(FakeTranscriber.identity, 'second')
    document, sources = store.snapshot(first)
    original = store.prepare_original(first, document, sources)
    with pytest.raises(TerminalError, match='source_claim_mismatch'):
        store.start_segment(second, original, sources[0])
    with pytest.raises(TerminalError, match='source_claim_mismatch'):
        store.publish(second, original, sources[0], SegmentText('wrong owner'), document)
    assert not texts(store)


def test_exact_result_retry_is_idempotent_and_conflicts_are_rejected(setup):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    claim = store.claim(FakeTranscriber.identity, 'worker')
    document, sources = store.snapshot(claim)
    original = store.prepare_original(claim, document, sources)
    source = sources[0]
    store.start_segment(claim, original, source)
    store.publish(claim, original, source, SegmentText('synthetic original'), document)
    before = [tuple(r) for r in texts(store)]
    store.publish(claim, original, source, SegmentText('synthetic original'), document)
    with pytest.raises(TerminalError, match='conflicting_result'):
        store.publish(claim, original, source, SegmentText('replacement'), document)
    assert [tuple(r) for r in texts(store)] == before


@pytest.mark.parametrize('boundary', ['original.before_commit', 'segment_result.before_commit', 'complete.before_commit', 'renew.before_commit'])
def test_expiry_at_commit_boundary_rolls_back_and_fences_result(setup, boundary):
    store, _, clock = setup
    triggered = False

    def hook(label):
        nonlocal triggered
        if label == boundary and not triggered:
            triggered = True
            clock.advance()

    store.hook = hook
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() == 'lost_claim'
    assert state(store) == 'running'
    assert store.db.execute('SELECT state FROM processing_jobs').fetchone()[0] == 'running'
    if boundary == 'segment_result.before_commit':
        assert not texts(store)
    assert store.recover() == 1


def test_actual_sqlite_commit_rejection_does_not_publish_success(setup):
    store, _, clock = setup
    reject = False

    def authorizer(action, first, second, db, trigger):
        if reject and action == sqlite3.SQLITE_TRANSACTION and first == 'COMMIT':
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def hook(label):
        nonlocal reject
        if label == 'segment_result.before_commit':
            reject = True

    store.db.set_authorizer(authorizer)
    store.hook = hook
    with pytest.raises(sqlite3.DatabaseError):
        TranscriptionWorker(store, FakeTranscriber()).run_one()
    assert not texts(store) and state(store) == 'running'
    store.db.set_authorizer(None)
    store.hook = lambda label: None
    clock.advance()
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() == JobState.COMPLETED


def test_multiple_intervals_and_supplemental_entry_provenance(external, checkout):
    root = external / 'data'
    first = recording(checkout, root, frames=1)
    with SegmentStore(repository=checkout, root=root) as audio:
        entry = audio.db.execute('SELECT entry_id FROM recording_sessions WHERE session_id=?', (first,)).fetchone()[0]
        sid, interval = audio.begin_session(audio_format=AudioFormat(sample_rate=1), entry_id=entry)
        fake = FakeMicrophone(AudioFormat(sample_rate=1))
        fake.start()
        writer = audio.writer(sid, interval)
        writer.append(fake.read(1))
        writer.pause()
        interval = audio.resume_session(sid)
        writer = audio.writer(sid, interval)
        # Fake signal start_frame is lifetime-relative; storage uses interval-relative frames.
        from personal_log.recording.fake import AudioBlock
        block = fake.read(1)
        writer.append(AudioBlock(0, block.frames, block.pcm))
        writer.finish(enqueue=True)
    with TranscriptionStore(repository=checkout, root=root) as store:
        worker = TranscriptionWorker(store, FakeTranscriber())
        assert worker.run_one() == JobState.COMPLETED
        assert worker.run_one() == JobState.COMPLETED
        rows = store.db.execute('SELECT source_json,entry_id FROM original_transcripts WHERE session_id=?', (sid,)).fetchone()
        doc = json.loads(rows[0])
        assert rows[1] == entry
        assert [s['interval_sequence'] for s in doc['segments']] == [0, 1]
        assert [s['start_frame'] for s in doc['segments']] == [0, 0]


def test_repaired_recording_is_not_automatically_eligible(setup, checkout):
    store, sid, _ = setup
    relative = store.db.execute('SELECT relative_path FROM audio_segments WHERE sequence=0').fetchone()[0]
    (store.root / relative).unlink()
    with SegmentStore(repository=checkout, root=store.root) as audio:
        audio.recover()
        assert audio.segments(sid)  # A recovery result is not a clean transcription authorization.
    assert store.enqueue(FakeTranscriber.identity) == 0
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() is None


def test_audio_recovery_blocks_owned_job_without_unblocking_it(setup, checkout):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    claim = store.claim(FakeTranscriber.identity, 'worker')
    relative = store.db.execute('SELECT relative_path FROM audio_segments WHERE sequence=0').fetchone()[0]
    (store.root / relative).unlink()
    with SegmentStore(repository=checkout, root=store.root) as audio:
        audio.recover()
    with pytest.raises(LostClaim):
        store.renew(claim)
    assert store.recover() == 1
    assert state(store) == 'terminal_failure'
    assert store.db.execute('SELECT state FROM processing_jobs').fetchone()[0] == 'blocked'


def test_engine_identity_is_frozen_and_changed_models_do_not_take_existing_jobs(setup):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    different = EngineIdentity('different', '2', 'different-model', '3', '{"local_only":true}')
    assert store.enqueue(different) == 0
    assert store.claim(different, 'wrong-engine') is None
    with pytest.raises(sqlite3.IntegrityError):
        store.db.execute('UPDATE transcription_jobs SET engine_json=?', (different.json(),))


def test_crashed_retry_cannot_exceed_attempt_budget(setup):
    store, _, clock = setup
    store.enqueue(FakeTranscriber.identity)
    for n in range(3):
        assert store.claim(FakeTranscriber.identity, 'worker')
        clock.advance()
        assert store.recover() == 1
        assert store.recover() == 0
    assert state(store) == 'terminal_failure'
    assert store.claim(FakeTranscriber.identity, 'worker') is None


@pytest.mark.parametrize('current,target', list(product(JobState, repeat=2)))
def test_sql_lifecycle_matches_validated_contract(setup, current, target):
    store, _, _ = setup
    store.enqueue(FakeTranscriber.identity)
    if current == JobState.COMPLETED:
        TranscriptionWorker(store, FakeTranscriber()).run_one()
    elif current != JobState.PENDING:
        claim = store.claim(FakeTranscriber.identity, 'worker')
        if current != JobState.RUNNING:
            store.fail(claim, current, 'synthetic_failure')
    if current == target:
        sql, values = 'UPDATE transcription_jobs SET state=?', (target,)
    elif target == JobState.RUNNING:
        sql = "UPDATE transcription_jobs SET state=?,attempts=attempts+1,owner='next',claim_token='next',lease_expires_at='2026-10-15T12:05:00Z'"
        values = (target,)
    else:
        sql = "UPDATE transcription_jobs SET state=?,owner=NULL,claim_token=NULL,lease_expires_at=NULL,error_code='synthetic_failure'"
        values = (target,)
    if current == target or target in TRANSITIONS[current]:
        store.db.execute(sql, values)
    else:
        with pytest.raises(sqlite3.IntegrityError):
            store.db.execute(sql, values)


def test_inference_does_not_hold_recorder_lock_or_write_transaction(setup, checkout):
    store, _, _ = setup
    entered, release = threading.Event(), threading.Event()

    class WaitingEngine(FakeTranscriber):
        def transcribe(self, audio, source, heartbeat):
            entered.set()
            assert release.wait(5)
            return super().transcribe(audio, source, heartbeat)

    def process():
        with TranscriptionStore(repository=checkout, root=store.root) as other:
            return TranscriptionWorker(other, WaitingEngine()).run_one()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(process)
        try:
            assert entered.wait(5)
            # Recording a new session succeeds while the old session's engine
            # is intentionally stalled; no lifetime root lock/SQL write lock.
            new_session = recording(checkout, store.root, frames=1)
            assert store.db.execute('SELECT state FROM recording_sessions WHERE session_id=?', (new_session,)).fetchone()[0] == 'finished'
        finally:
            release.set()
        assert future.result(timeout=5) == JobState.COMPLETED


@pytest.mark.parametrize('bad_state', ['writing', 'unresolved', 'recovered'])
def test_nonfinalized_sources_are_never_consumed(setup, bad_state):
    store, _, _ = setup
    store.db.execute('UPDATE audio_segments SET state=? WHERE sequence=1', (bad_state,))
    engine = FakeTranscriber()
    assert TranscriptionWorker(store, engine).run_one() == JobState.TERMINAL
    assert not engine.calls


@pytest.mark.parametrize('damage', ['frame_gap', 'open_interval', 'revision_changed'])
def test_invalid_capture_history_cannot_complete(setup, damage):
    store, _, _ = setup
    if damage == 'frame_gap':
        store.db.execute('UPDATE audio_segments SET start_frame=25 WHERE sequence=1')
    elif damage == 'open_interval':
        store.db.execute('UPDATE capture_intervals SET ended_at=NULL,end_reason=NULL')
    else:
        # Adopt first so changing revision must prevent claims, not silently
        # reinterpret the old job as a request for newer audio.
        store.enqueue(FakeTranscriber.identity)
        store.db.execute('UPDATE recording_sessions SET input_revision=input_revision+1')
    engine = FakeTranscriber()
    result = TranscriptionWorker(store, engine).run_one()
    assert result == (None if damage == 'revision_changed' else JobState.TERMINAL)
    assert not engine.calls and not texts(store)


def test_lease_expiry_during_inference_discards_late_output(setup):
    store, _, clock = setup

    class SlowEngine(FakeTranscriber):
        def transcribe(self, audio, source, heartbeat):
            result = super().transcribe(audio, source, heartbeat)
            clock.advance()
            return result

    assert TranscriptionWorker(store, SlowEngine()).run_one() == 'lost_claim'
    assert not texts(store)
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() == JobState.COMPLETED


@pytest.mark.parametrize('signal', [KeyboardInterrupt, SystemExit])
def test_explicit_worker_termination_is_recoverable(setup, signal):
    store, _, _ = setup

    class StoppedEngine(FakeTranscriber):
        def transcribe(self, audio, source, heartbeat):
            raise signal()

    with pytest.raises(signal):
        TranscriptionWorker(store, StoppedEngine()).run_one()
    assert state(store) == 'interrupted'
    assert TranscriptionWorker(store, FakeTranscriber()).run_one() == JobState.COMPLETED


def test_unexpected_engine_errors_do_not_log_transcript_contents(setup, capsys):
    store, _, _ = setup

    class BrokenEngine(FakeTranscriber):
        def transcribe(self, audio, source, heartbeat):
            raise ValueError('synthetic private words must not enter diagnostics')

    assert TranscriptionWorker(store, BrokenEngine()).run_one() == JobState.TERMINAL
    assert store.db.execute('SELECT error_code FROM transcription_jobs').fetchone()[0] == 'engine_exception'
    captured = capsys.readouterr()
    assert not captured.out and not captured.err


def test_empty_finished_recording_is_explicit_terminal_failure(external, checkout):
    root = external / 'data'
    recording(checkout, root, frames=0)
    with TranscriptionStore(repository=checkout, root=root) as store:
        assert TranscriptionWorker(store, FakeTranscriber()).run_one() == JobState.TERMINAL
        assert store.db.execute('SELECT error_code FROM transcription_jobs').fetchone()[0] == 'empty_recording'
