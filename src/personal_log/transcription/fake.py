"""Synthetic text only; no ASR, model, downloads, microphone, or network."""

from .contracts import EngineIdentity, EngineInterrupted, RetryableError, SegmentText, TerminalError


class FakeTranscriber:
    identity = EngineIdentity('personal-log-fake', '1', 'synthetic-only', '1')

    def __init__(self, *, failures=None):
        # Per-sequence failure scripts are adapter test controls, not queue policy.
        self.failures = {key: list(value) for key, value in (failures or {}).items()}
        self.calls = []

    def transcribe(self, audio, source, heartbeat):
        heartbeat()
        self.calls.append(source.segment_id)
        actions = self.failures.get(source.sequence, [])
        action = actions.pop(0) if actions else 'success'
        if action == 'retry':
            raise RetryableError('synthetic_retry')
        if action == 'terminal':
            raise TerminalError('synthetic_terminal')
        if action == 'interrupt':
            raise EngineInterrupted('synthetic_interruption')
        if action != 'success':
            raise ValueError('Unknown synthetic failure action')
        if audio.read(4) != b'RIFF':
            raise TerminalError('invalid_audio')
        return SegmentText(f'[synthetic segment {source.sequence}; source {source.sha256}]')
