"""No PortAudio import or physical microphone access: simulated raw callbacks."""

import struct
import json
import queue
import threading
import time
import wave

import pytest

from personal_log.recording.fake import AudioFormat, CaptureFailure
from personal_log.recording.microphone import Microphone, discover_inputs
from personal_log.recording import process as process_module
from personal_log.recording.process import RecorderConfig, RecorderProcess
from personal_log.storage.segments import SegmentStore
from test_recorder_process import config, await_frames, await_exit, query


class Flags:
    def __init__(self, overflow=False):
        self.input_overflow = overflow

    def __bool__(self):
        return self.input_overflow


class SimulatedBackend:
    class CallbackAbort(Exception):
        pass

    def __init__(self, *, mode='', automatic=False):
        self.mode, self.automatic = mode, automatic
        self.counter = 0
        self.streams = []

    def query_hostapis(self, index=None):
        hosts = [{'name': 'MME'}, {'name': 'Windows WASAPI'}]
        return hosts if index is None else hosts[index]

    def query_devices(self, index=None):
        devices = [dict(name='Simulated MME', hostapi=0, max_input_channels=1, default_samplerate=44100),
                   dict(name='Simulated WASAPI', hostapi=1, max_input_channels=2, default_samplerate=48000),
                   dict(name='Output only', hostapi=0, max_input_channels=0, default_samplerate=48000)]
        if self.mode == 'identity_changed':
            devices[1]['name'] = 'Replacement microphone'
        return devices if index is None else devices[index]

    def check_input_settings(self, **kwargs):
        if self.mode == 'unsupported' or kwargs['samplerate'] not in {100, 48000}:
            raise ValueError('Unsupported mono PCM16 rate')

    def RawInputStream(self, **kwargs):
        if self.mode == 'initialization':
            raise OSError('Device unavailable')
        stream = SimulatedStream(self, kwargs)
        self.streams.append(stream)
        return stream


class SimulatedStream:
    def __init__(self, api, args):
        self.api, self.args = api, args
        self.active = False
        self.thread = None
        self.closed = False

    def emit(self, frames=None, status=None, data=None):
        frames = frames or self.args['blocksize']
        start = self.api.counter
        self.api.counter += frames
        data = data if data is not None else b''.join(struct.pack('<h', n % 30000) for n in range(start, start + frames))
        self.args['callback'](data, frames, None, status or Flags())

    def start(self):
        self.active = True
        if self.api.automatic:
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def run(self):
        count = 0
        try:
            while self.active:
                time.sleep(0.01)
                if not self.active:
                    break
                count += 1
                if self.api.mode == 'no_callbacks':
                    continue
                if self.api.mode == 'saturation':
                    for _ in range(100):
                        self.emit()
                if count == 8 and self.api.mode == 'disconnect':
                    self.active = False
                    self.args['finished_callback']()
                    return
                self.emit(status=Flags(count == 8 and self.api.mode == 'overflow'))
        except self.api.CallbackAbort:
            self.active = False
            self.args['finished_callback']()

    def stop(self):
        self.active = False
        if self.thread:
            self.thread.join(1)
        # A callback already in flight when Stop arrived must precede the barrier.
        if self.api.mode == 'stop_tail':
            self.emit()
        if self.api.mode == 'stop_overflow':
            try:
                self.emit(status=Flags(True))
            except self.api.CallbackAbort:
                pass
        self.args['finished_callback']()

    def close(self):
        self.closed = True


def microphone(api=None, **kwargs):
    return Microphone(AudioFormat(sample_rate=100), device=1, device_name='Simulated WASAPI',
        host_api='Windows WASAPI', block_frames=2, buffer_blocks=3, api=api or SimulatedBackend(), **kwargs)


def test_discovery_prefers_wasapi_but_preserves_other_host_apis():
    devices = discover_inputs(SimulatedBackend())
    assert [d.index for d in devices] == [1, 0]
    assert devices[0].supported_rates == (48000,)
    assert devices[0].max_channels == 2


@pytest.mark.parametrize('mode', ['unsupported', 'initialization', 'identity_changed'])
def test_invalid_device_or_format_fails_without_fallback(mode):
    source = microphone(SimulatedBackend(mode=mode))
    with pytest.raises(CaptureFailure, match='initialization failed'):
        source.start()
    assert not source.running
    assert source.error == 'initialization_failed'


def test_callback_order_and_resume_have_no_overlap():
    source = microphone()
    source.start()
    first_stream = source.stream
    first_stream.emit()
    first_stream.emit()
    source.stop()
    blocks = [source.read(2), source.read(2)]
    assert source.read(2) is None
    source.start()
    source.stream.emit()
    source.stop()
    blocks.append(source.read(2))
    assert [b.start_frame for b in blocks] == [0, 2, 4]
    assert b''.join(b.pcm for b in blocks) == struct.pack('<6h', *range(6))
    assert first_stream.closed


def test_bounded_callback_buffer_accounts_rejected_frames():
    source = microphone()
    source.start()
    for _ in range(3):
        source.stream.emit()
    with pytest.raises(source.api.CallbackAbort):
        source.stream.emit()
    assert len(source.blocks) == 3
    assert source.position == 8
    assert source.failure() == 'buffer_saturated'
    source.stop()
    with pytest.raises(CaptureFailure):
        source.start()


@pytest.mark.parametrize('mode', ['overflow', 'exception', 'oversize', 'disconnect', 'inactive', 'timeout'])
def test_callback_failures_are_latched(mode):
    source = microphone()
    source.start()
    if mode == 'disconnect':
        source.stream.args['finished_callback']()
    elif mode == 'inactive':
        source.stream.active = False
    elif mode == 'timeout':
        source.last_callback = time.monotonic() - 3
    else:
        with pytest.raises(source.api.CallbackAbort):
            source.stream.emit(frames=3 if mode == 'oversize' else 2,
                               status=Flags(mode == 'overflow'), data=object() if mode == 'exception' else None)
    assert source.failure()
    assert len(source.blocks) == 0
    assert source.unknown_loss == (mode == 'overflow')
    source.stop()


def _simulated_worker(sock, settings, repository, root, owner):
    from personal_log.recording import worker
    api = SimulatedBackend(mode=settings.fault_boundary or '', automatic=True)
    worker.create_capture = lambda cfg: Microphone(AudioFormat(sample_rate=cfg.sample_rate),
        device=1, device_name='Simulated WASAPI', host_api='Windows WASAPI',
        block_frames=2, buffer_blocks=cfg.queue_blocks, api=api)
    worker.worker_main(sock, settings, repository, root, owner)


def _simulated_supervisor(*args):
    from personal_log.recording import worker
    worker.worker_main = _simulated_worker
    process_module._supervisor(*args)


def real_config(**kwargs):
    return config(capture_source='microphone', input_device=1, input_device_name='Simulated WASAPI',
                  input_host_api='Windows WASAPI', **kwargs)


def test_spawned_microphone_rapid_pause_resume_finish_drains_tail(monkeypatch, external, checkout):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config(fault_boundary='stop_tail')) as recorder:
        assert recorder.request('record')['payload']['state'] == 'recording'
        await_frames(recorder)
        for _ in range(3):
            assert recorder.request('pause')['payload']['state'] == 'paused'
            assert recorder.request('resume')['payload']['state'] == 'recording'
        assert recorder.request('finish')['payload']['state'] == 'finished'
        sid = recorder.session_id
    with SegmentStore(repository=checkout, root=root) as store:
        blocks = []
        for ref in store.segments(sid):
            with wave.open(str(root / ref['relative_path']), 'rb') as source:
                blocks.append(source.readframes(source.getnframes()))
        pcm = b''.join(blocks)
        assert len(pcm) >= 20
        assert pcm == b''.join(struct.pack('<h', n) for n in range(len(pcm) // 2))
        assert store.db.execute('SELECT count(*) FROM capture_intervals').fetchone()[0] == 4
        identities = [json.loads(row[0]) for row in store.db.execute('SELECT device_id FROM capture_intervals')]
        assert all(identity == dict(backend='portaudio', index=1, name='Simulated WASAPI',
                                    host_api='Windows WASAPI') for identity in identities)
        assert store.db.execute('SELECT count(*) FROM processing_jobs').fetchone()[0] == 1


@pytest.mark.parametrize('mode', ['disconnect', 'overflow', 'initialization', 'saturation'])
def test_spawned_device_failure_never_finishes_and_recovery_does_not_restart(monkeypatch, external, checkout, mode):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config(fault_boundary=mode)) as recorder:
        recorder.send(recorder.make_command('record'))
        events = await_exit(recorder, heartbeat=True)
        assert any(e['payload'].get('error_code') == 'capture_failed' for e in events)
    assert query(root, 'SELECT state FROM recording_sessions') == [('interrupted',)]
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]
    assert query(root, 'SELECT count(*) FROM recovery_findings') != [(0,)]
    with RecorderProcess(repository=checkout, root=root, config=real_config()) as recorder:
        assert recorder.descriptor['recovery_findings'] > 0
        time.sleep(0.05)
        assert query(root, 'SELECT count(*) FROM recording_sessions') == [(1,)]


@pytest.mark.parametrize('changes', [dict(input_device=None), dict(input_device_name=''),
    dict(input_host_api=''), dict(synthetic_speed=2), dict(capture_fail_at=2)])
def test_real_configuration_requires_identity_and_rejects_synthetic_controls(changes):
    values = dict(capture_source='microphone', input_device=1, input_device_name='Simulated WASAPI',
                  input_host_api='Windows WASAPI')
    values.update(changes)
    with pytest.raises(ValueError):
        RecorderConfig(**values)


@pytest.mark.parametrize('boundary,mode', [('pcm.before_write', 'fail'), ('pcm.before_write', 'block'),
                                         ('pcm.written', 'crash')])
def test_simulated_microphone_storage_failure_and_worker_crash(monkeypatch, external, checkout, boundary, mode):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root,
            config=real_config(fault_boundary=boundary, fault_mode=mode, queue_blocks=4, drain_seconds=0.4)) as recorder:
        recorder.send(recorder.make_command('record'))
        events = await_exit(recorder, heartbeat=True)
        assert any(e['payload']['code'] == 'interrupted' for e in events)
        assert not any(e['payload']['state'] == 'finished' for e in events)
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]
    with SegmentStore(repository=checkout, root=root) as store:
        store.recover()
        before = store.db.execute('SELECT count(*) FROM recovery_findings').fetchone()[0]
        store.recover()
        assert store.db.execute('SELECT count(*) FROM recovery_findings').fetchone()[0] == before
        assert store.db.execute('SELECT state FROM recording_sessions').fetchone()[0] == 'recovery_required'


@pytest.mark.parametrize('reason', ['ui_crash', 'windows_shutdown'])
def test_simulated_microphone_supervisor_interruption(monkeypatch, external, checkout, reason):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config()) as recorder:
        recorder.request('record')
        await_frames(recorder)
        assert recorder.request('shutdown', reason=reason)['payload']['state'] == 'interrupted'
        await_exit(recorder)
    assert query(root, 'SELECT interruption_code FROM recording_sessions') == [(reason,)]
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]


def test_simulated_microphone_supervisor_killed_worker_exits_and_releases_root(monkeypatch, external, checkout):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config()) as recorder:
        recorder.request('record')
        await_frames(recorder)
        recorder.process.terminate()
        recorder.process.join(2)
        # Worker notices its private channel closure; retry the OS root lock until exit.
        until = time.monotonic() + 3
        while True:
            try:
                with SegmentStore(repository=checkout, root=root) as store:
                    store.recover()
                    assert store.db.execute('SELECT state FROM recording_sessions').fetchone()[0] == 'recovery_required'
                break
            except Exception:
                if time.monotonic() >= until:
                    raise
                time.sleep(0.05)
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]


def test_finish_with_overflow_in_stop_callback_cannot_ack_success(monkeypatch, external, checkout):
    # The source discovers loss while stopping, after Finish was prepared.
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config(fault_boundary='stop_overflow')) as recorder:
        recorder.request('record')
        await_frames(recorder)
        finish = recorder.make_command('finish')
        recorder.send(finish)
        events = await_exit(recorder, heartbeat=True)
        assert not any(e['payload'].get('command_id') == finish['message_id'] and e['payload']['state'] == 'finished'
                       for e in events)
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]
    assert query(root, "SELECT count(*) FROM recovery_findings WHERE code='input_overflow'") == [(1,)]


def test_cli_requires_explicit_recording_opt_in(monkeypatch, capsys):
    from personal_log.recording.cli import main
    monkeypatch.setattr('personal_log.recording.cli.discover_inputs', lambda: pytest.fail('Should not probe devices'))
    with pytest.raises(SystemExit):
        main(['record'])
    assert 'UNENCRYPTED' in capsys.readouterr().out


def test_backend_status_query_error_is_capture_failure():
    source = microphone()
    source.start()

    class BrokenStatus:
        @property
        def active(self):
            raise OSError('Device handle invalid')

        def stop(self):
            pass

        def close(self):
            pass

    source.stream = BrokenStatus()
    assert source.failure() == 'device_status_failed'
    source.stop()


def test_resume_revalidates_device_identity_without_switching():
    api = SimulatedBackend()
    source = microphone(api)
    source.start()
    source.stop()
    api.mode = 'identity_changed'
    with pytest.raises(CaptureFailure, match='identity changed'):
        source.start()
    assert len(api.streams) == 1


def test_initialization_failure_returns_clear_rejection(monkeypatch, external, checkout):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root, config=real_config(fault_boundary='initialization')) as recorder:
        response = recorder.request('record')['payload']
        assert response['code'] == 'rejected'
        assert response['error_code'] == 'capture_failed'
        assert response['state'] == 'interrupted'
        await_exit(recorder)
    assert query(root, 'SELECT count(*) FROM processing_jobs') == [(0,)]


def test_active_stream_without_callbacks_keeps_worker_ticks(monkeypatch, external, checkout):
    monkeypatch.setattr(process_module, '_supervisor', _simulated_supervisor)
    root = external / 'data'
    with RecorderProcess(repository=checkout, root=root,
            config=real_config(fault_boundary='no_callbacks', worker_timeout=0.3, grace_seconds=3)) as recorder:
        recorder.request('record')
        events = await_exit(recorder, heartbeat=True)
        assert any(e['payload'].get('error_code') == 'capture_failed' for e in events)
        assert not any(e['payload'].get('error_code') == 'storage_failed' for e in events)
    assert query(root, "SELECT count(*) FROM recovery_findings WHERE code='device_disconnected'") == [(1,)]


@pytest.mark.parametrize('terminal', ['finish', 'interrupt'])
def test_console_terminal_command_is_retained_under_backpressure(terminal):
    from personal_log.recording.cli import _read_commands
    lines = queue.Queue(2)
    inputs = iter(['pause', 'resume', 'ignored', terminal])
    reader = threading.Thread(target=_read_commands, args=(lines, lambda: next(inputs)), daemon=True)
    reader.start()
    until = time.monotonic() + 1
    while lines.qsize() < 2 and time.monotonic() < until:
        time.sleep(0.001)
    assert lines.get(timeout=1) == 'pause'
    assert lines.get(timeout=1) == 'resume'
    assert lines.get(timeout=1) == terminal
    reader.join(1)
    assert not reader.is_alive()
