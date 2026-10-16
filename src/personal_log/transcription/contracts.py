"""Version 1 engine and durable execution contracts, independent of recording IPC."""

from dataclasses import asdict, dataclass
from enum import StrEnum
import json
from typing import BinaryIO, Callable, Protocol


CONTRACT_VERSION = 1


class JobState(StrEnum):
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    RETRYABLE = 'retryable_failure'
    TERMINAL = 'terminal_failure'
    INTERRUPTED = 'interrupted'


TRANSITIONS = {
    JobState.PENDING: {JobState.RUNNING},
    JobState.RUNNING: {JobState.COMPLETED, JobState.RETRYABLE, JobState.TERMINAL, JobState.INTERRUPTED},
    JobState.RETRYABLE: {JobState.RUNNING},
    JobState.INTERRUPTED: {JobState.RUNNING},
    JobState.COMPLETED: set(), JobState.TERMINAL: set(),
}


def transition(current, target):
    current, target = JobState(current), JobState(target)
    if target not in TRANSITIONS[current]:
        raise ValueError('Invalid transcription transition')
    return target


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class EngineIdentity:
    engine: str
    engine_version: str
    model: str
    model_version: str
    configuration: str = '{}'

    def __post_init__(self):
        if any(not isinstance(x, str) or not x.strip() for x in
               (self.engine, self.engine_version, self.model, self.model_version)):
            raise ValueError('Explicit engine and model identity required')
        value = json.loads(self.configuration)
        if not isinstance(value, dict):
            raise ValueError('Configuration must be a JSON object')
        object.__setattr__(self, 'configuration', canonical(value))

    def json(self):
        return canonical({**asdict(self), 'configuration': json.loads(self.configuration), 'schema_version': CONTRACT_VERSION})


@dataclass(frozen=True)
class Source:
    segment_id: str
    interval_id: str
    sequence: int
    interval_sequence: int
    start_frame: int
    frame_count: int
    byte_length: int
    sha256: str
    relative_path: str
    sample_rate: int
    channels: int
    sample_width: int
    interval_started_at: str
    interval_ended_at: str
    device_id: str


@dataclass(frozen=True)
class SegmentText:
    text: str

    def __post_init__(self):
        if not isinstance(self.text, str):
            raise ValueError('Engine must return text, including empty text for silence')


class Engine(Protocol):
    identity: EngineIdentity

    def transcribe(self, audio: BinaryIO, source: Source, heartbeat: Callable[[], None]) -> SegmentText: ...


class TranscriptionError(RuntimeError):
    """Sanitized classification only: do not log engine messages or speech."""

    def __init__(self, code='engine_failure'):
        if not isinstance(code, str) or not code or len(code) > 64 or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in code):
            raise ValueError('Invalid error classification')
        self.code = code
        super().__init__(code)


class RetryableError(TranscriptionError):
    pass


class TerminalError(TranscriptionError):
    pass


class EngineInterrupted(TranscriptionError):
    pass


class LostClaim(RuntimeError):
    pass
