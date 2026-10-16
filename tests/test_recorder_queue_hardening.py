"""Synthetic queue rejection at the actual process boundaries."""

import json
from pathlib import Path
import queue

import pytest

from personal_log.recording import process as process_module
from personal_log.recording.process import RecorderProcess
from test_recorder_process import config, await_frames, await_exit, query


def _injected_worker(sock, settings, repository, root, owner):
    from personal_log.recording import worker

    original_queue = queue.Queue
    mode = settings.fault_boundary

    class RejectingQueue:
        def __init__(self, size):
            self.inner = original_queue(size)
            self.attempts = 0

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def put_nowait(self, task):
            if task['type'] == mode:
                self.attempts += 1
                # Keep two accepted audio blocks before rejecting the third.
                reject = self.attempts == 3 if mode == 'audio' else self.attempts <= 3
                with (Path(root) / 'task-attempts.jsonl').open('a') as log:
                    log.write(json.dumps({'type': mode, 'rejected': reject}) + '\n')
                if reject:
                    raise queue.Full
            return self.inner.put_nowait(task)

    worker.queue.Queue = lambda size: RejectingQueue(size) if size == settings.queue_blocks + 8 else original_queue(size)
    worker.worker_main(sock, settings, repository, root, owner)


def _queue_supervisor(*args):
    from personal_log.recording import worker
    worker.worker_main = _injected_worker
    process_module._supervisor(*args)


def _rejected_stop_supervisor(*args):
    original = process_module.JsonChannel
    root, settings = Path(args[2]), args[5]

    class RejectingChannel(original):
        attempts = 0

        def queue(self, value):
            if value.get('type') == 'stop':
                self.attempts += 1
                rejected = settings.fault_boundary == 'reject_all_stops' or self.attempts == 1
                with (root / 'stop-attempts.jsonl').open('a') as log:
                    log.write(json.dumps({'message': value, 'rejected': rejected}) + '\n')
                if rejected:
                    return False
            return super().queue(value)

    process_module.JsonChannel = RejectingChannel
    process_module._supervisor(*args)


@pytest.mark.parametrize('kind', ['prepare', 'execute', 'audio'])
def test_task_saturation_interrupts_without_unhandled_exception(monkeypatch, external, checkout, kind):
    monkeypatch.setattr(process_module, '_supervisor', _queue_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary=kind)) as recorder:
        recorder.send(recorder.make_command('record'))
        events = await_exit(recorder, heartbeat=True)
        assert recorder.process.exitcode == 0
        assert any(e['payload'].get('code') in {'stopped', 'interrupted'} for e in events), events
        assert not any(e['payload'].get('error_code') == 'recorder_crash' for e in events)
        if kind == 'audio':
            assert any(e['payload'].get('error_code') == 'storage_failed' for e in events)
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]
    if kind == 'audio':
        assert query(root, 'SELECT state FROM recording_sessions') == [('interrupted',)]
        assert query(root, "SELECT count(*) FROM recovery_findings WHERE code='buffered_capture_gap'") == [(1,)]
        assert query(root, 'SELECT sum(frame_count) FROM audio_segments') == [(4,)]


def test_saturated_stop_task_retries_behind_accepted_audio(monkeypatch, external, checkout):
    monkeypatch.setattr(process_module, '_supervisor', _queue_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary='stop')) as recorder:
        recorder.request('record')
        await_frames(recorder)
        assert recorder.request('shutdown')['payload']['state'] == 'finished'
        await_exit(recorder)
        assert recorder.process.exitcode == 0
    attempts = [json.loads(line) for line in (root / 'task-attempts.jsonl').read_text().splitlines()]
    assert [a['rejected'] for a in attempts] == [True, True, True, False]
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(1,)]
    assert query(root, 'SELECT sum(frame_count) FROM audio_segments')[0][0] >= 10


@pytest.mark.parametrize('always', [False, True])
def test_rejected_internal_stop_is_retried_without_extending_deadline(monkeypatch, external, checkout, always):
    monkeypatch.setattr(process_module, '_supervisor', _rejected_stop_supervisor)
    root = external / 'data'
    mode = 'reject_all_stops' if always else 'reject_first_stop'
    with RecorderProcess(repository=checkout, root=root, config=config(fault_boundary=mode, drain_seconds=0.4)) as recorder:
        recorder.request('record')
        await_frames(recorder)
        message = recorder.make_command('shutdown')
        recorder.send(message)
        if not always:
            assert recorder.wait(message['message_id'])['payload']['state'] == 'finished'
        await_exit(recorder, timeout=3)
        assert recorder.process.exitcode == 0
    attempts = [json.loads(line) for line in (root / 'stop-attempts.jsonl').read_text().splitlines()]
    assert len(attempts) >= 2
    assert attempts[0]['rejected']
    assert all(a['message'] == attempts[0]['message'] for a in attempts)
    assert attempts[0]['message']['message']['message_id'] == message['message_id']
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0 if always else 1,)]
