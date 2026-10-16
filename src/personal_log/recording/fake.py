"""Pull-driven synthetic capture: no device, clock, thread, or file access."""

from dataclasses import dataclass
import struct
from typing import Protocol


@dataclass(frozen=True)
class AudioFormat:
    sample_rate: int = 48_000
    channels: int = 1
    sample_width: int = 2

    def __post_init__(self):
        if type(self.sample_rate) is not int or self.sample_rate <= 0:
            raise ValueError("sample_rate must be a positive integer")
        if type(self.channels) is not int or type(self.sample_width) is not int or self.channels != 1 or self.sample_width != 2:
            raise ValueError("Increment 1 supports mono PCM16 only")


@dataclass(frozen=True)
class AudioBlock:
    start_frame: int
    frames: int
    pcm: bytes


class Capture(Protocol):
    format: AudioFormat

    def start(self) -> None: ...
    def read(self, frames: int) -> AudioBlock: ...
    def stop(self) -> None: ...


class CaptureFailure(RuntimeError):
    """Synthetic capture interruption at an exact frame boundary."""


class FakeMicrophone:
    """Frame n contains signed little-endian PCM16 (n mod 65536) - 32768.

    stop/start models pause/resume; the next frame remains continuous. An
    interruption returns the prefix before fail_at_frame, then raises on the
    next read. No samples are fabricated beyond the failure boundary.
    """

    def __init__(self, audio_format: AudioFormat | None = None, *, fail_at_frame: int | None = None):
        if fail_at_frame is not None and (type(fail_at_frame) is not int or fail_at_frame < 0):
            raise ValueError("fail_at_frame must be a nonnegative integer")
        self.format = audio_format or AudioFormat()
        self.fail_at_frame = fail_at_frame
        self.position = 0
        self.running = False

    def start(self) -> None:
        if self.running:
            raise RuntimeError("Capture already started")
        self.running = True

    def stop(self) -> None:
        self.running = False

    def read(self, frames: int) -> AudioBlock:
        if not self.running:
            raise RuntimeError("Capture is stopped")
        if type(frames) is not int or not 0 < frames <= self.format.sample_rate:
            raise ValueError("Read size must be between one frame and one second")
        if self.fail_at_frame is not None:
            if self.position >= self.fail_at_frame:
                self.running = False
                raise CaptureFailure("Synthetic microphone disconnected")
            frames = min(frames, self.fail_at_frame - self.position)
        start = self.position
        pcm = b"".join(struct.pack("<h", (n % 65536) - 32768) for n in range(start, start + frames))
        self.position += frames
        return AudioBlock(start, frames, pcm)
