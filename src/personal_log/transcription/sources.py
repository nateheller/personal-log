"""Complete, conservative input manifests; never salvage or mutate audio."""

from dataclasses import asdict
from hashlib import sha256

from personal_log.storage.files import WavInfo, valid_wav
from .contracts import Source, TerminalError, canonical


def manifest(db, job_id):
    session = db.execute(
        'SELECT s.*,p.input_revision AS job_revision,p.pipeline_version FROM processing_jobs p '
        'JOIN recording_sessions s USING(session_id) WHERE p.job_id=?', (job_id,)).fetchone()
    if (session is None or session['state'] != 'finished' or session['recovery_status'] != 'clean'
            or session['input_revision'] != session['job_revision'] or session['pipeline_version'] != 'capture-v1'):
        raise TerminalError('ineligible_recording')
    if db.execute("SELECT 1 FROM recovery_findings WHERE session_id=? AND severity<>'info' LIMIT 1",
                  (session['session_id'],)).fetchone():
        raise TerminalError('unresolved_recording')
    intervals = db.execute('SELECT * FROM capture_intervals WHERE session_id=? ORDER BY sequence',
                           (session['session_id'],)).fetchall()
    if not intervals or any(i['sequence'] != n or i['ended_at'] is None or i['end_reason'] not in {'pause', 'finish'}
                            or i['channels'] != 1 or i['sample_width'] != 2
                            for n, i in enumerate(intervals)):
        raise TerminalError('invalid_intervals')
    by_id = {i['interval_id']: i for i in intervals}
    rows = db.execute('SELECT s.*,t.accepted_frames,t.durable_frames,t.publication_sha256,'
        't.publication_frames,t.publication_bytes,r.segment_id AS repair_id '
        'FROM audio_segments s LEFT JOIN segment_storage t USING(segment_id) '
        'LEFT JOIN segment_recoveries r USING(segment_id) WHERE s.session_id=? ORDER BY s.sequence',
        (session['session_id'],)).fetchall()
    if not rows:
        raise TerminalError('empty_recording')
    sources, next_frame, previous_interval = [], {}, -1
    for n, row in enumerate(rows):
        i = by_id.get(row['interval_id'])
        if (i is None or row['sequence'] != n or row['state'] != 'finalized' or row['repair_id'] is not None
                or row['frame_count'] <= 0 or row['byte_length'] != 44 + row['frame_count'] * 2
                or not row['sha256'] or row['start_frame'] != next_frame.get(row['interval_id'], 0)
                or i['sequence'] < previous_interval or row['accepted_frames'] != row['frame_count']
                or row['durable_frames'] != row['frame_count'] or row['publication_frames'] != row['frame_count']
                or row['publication_sha256'] != row['sha256'] or row['publication_bytes'] != row['byte_length']):
            raise TerminalError('invalid_segment_manifest')
        previous_interval = i['sequence']
        next_frame[row['interval_id']] = row['start_frame'] + row['frame_count']
        sources.append(Source(row['segment_id'], row['interval_id'], n, i['sequence'], row['start_frame'],
            row['frame_count'], row['byte_length'], row['sha256'], row['relative_path'], i['sample_rate'],
            i['channels'], i['sample_width'], i['started_at'], i['ended_at'], i['device_id']))
    document = dict(schema_version=1, session_id=session['session_id'], entry_id=session['entry_id'],
        input_revision=session['input_revision'], session_created_at=session['created_at'],
        intervals=[dict(i) for i in intervals], segments=[asdict(s) for s in sources])
    return canonical(document), sources


def validate_file(root, source):
    expected = WavInfo(source.frame_count, source.byte_length, source.sha256)
    if not valid_wav(root, source.relative_path, source.sample_rate, expected):
        raise TerminalError('source_audio_invalid')


def validate_stream(audio, source):
    """Hash the actual open descriptor supplied to the engine, not just a path."""
    digest, length = sha256(), 0
    audio.seek(0)
    while chunk := audio.read(65536):
        digest.update(chunk)
        length += len(chunk)
    if length != source.byte_length or digest.hexdigest() != source.sha256:
        raise TerminalError('source_audio_changed')
    audio.seek(0)
