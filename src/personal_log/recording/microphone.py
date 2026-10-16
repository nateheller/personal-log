"""Explicit PortAudio input selection and bounded callback-to-worker handoff.

No device is opened on import. Only the recorder worker starts streams.
"""

from collections import deque
from dataclasses import dataclass
import time

from .fake import AudioBlock, AudioFormat, CaptureFailure


def backend():
    try:
        import sounddevice
        return sounddevice
    except (ImportError, OSError) as error:
        raise CaptureFailure('Install personal-log[microphone]; PortAudio must be available') from error


@dataclass(frozen=True)
class InputDevice:
    index: int
    name: str
    host_api: str
    max_channels: int
    default_rate: float
    supported_rates: tuple[int, ...]


def discover_inputs(api=None):
    """Probe mono PCM16 at common rates, without starting a recording stream."""
    api = api or backend()
    try:
        hosts = api.query_hostapis()
        devices = api.query_devices()
        found = []
        for index, info in enumerate(devices):
            if info['max_input_channels'] < 1:
                continue
            rates = []
            for rate in sorted({16000, 44100, 48000, int(info['default_samplerate'])}):
                if not 1 <= rate <= 192000:
                    continue
                try:
                    api.check_input_settings(device=index, samplerate=rate, channels=1, dtype='int16')
                    rates.append(rate)
                except Exception:
                    pass  # A probe failure means unsupported, not a fallback.
            found.append(InputDevice(index, info['name'], hosts[info['hostapi']]['name'],
                                     info['max_input_channels'], info['default_samplerate'], tuple(rates)))
        return sorted(found, key=lambda d: ('WASAPI' not in d.host_api.upper(), d.index))
    except Exception as error:
        raise CaptureFailure('Microphone discovery failed; check Windows audio permissions/devices') from error


class Microphone:
    """One selected input, mono little-endian PCM16, no resampling or fallback.

    Callback work is bounded by block_frames. A single producer appends immutable
    copies to a bounded deque; the worker is its sole consumer (CPython/GIL).
    No locks, logging, file/DB calls, hashing, or IPC occur in the callback.
    Python allocation/GIL scheduling is not a hard real-time guarantee.
    """

    def __init__(self, audio_format: AudioFormat, *, device: int, device_name: str,
                 host_api: str, block_frames: int, buffer_blocks: int, api=None):
        self.format = audio_format
        self.device, self.device_name, self.host_api = device, device_name, host_api
        self.block_frames, self.buffer_blocks = block_frames, buffer_blocks
        if type(device) is not int or device < 0 or not device_name or not host_api:
            raise ValueError('An explicit input index, name, and host API are required')
        if not 1 <= block_frames <= audio_format.sample_rate or not 2 <= buffer_blocks <= 1000:
            raise ValueError('Invalid bounded microphone buffer')
        self.api = api
        self.blocks = deque()
        self.stream = None
        self.running = False
        self.error = None
        self.position = 0  # Includes known rejected callback frames, across pauses.
        self.unknown_loss = False
        self.last_callback = 0.0
        self._expected_stop = False

    def _callback(self, data, frames, timing, status):
        try:
            start = self.position
            self.position += frames
            self.last_callback = time.monotonic()
            if status:
                self.unknown_loss = True
                self.error = 'input_overflow' if status.input_overflow else 'callback_status'
            if self.error:
                raise self.api.CallbackAbort
            if not 0 < frames <= self.block_frames or len(data) != frames * 2:
                self.error = 'callback_format'
                raise self.api.CallbackAbort
            if len(self.blocks) >= self.buffer_blocks:
                self.error = 'buffer_saturated'
                raise self.api.CallbackAbort
            self.blocks.append(AudioBlock(start, frames, bytes(data)))
        except self.api.CallbackAbort:
            raise
        except BaseException:
            self.error = 'callback_exception'
            raise self.api.CallbackAbort

    def _finished(self):
        if not self._expected_stop:
            self.error = self.error or 'device_disconnected'

    def start(self):
        if self.running or self.blocks or self.error:
            raise CaptureFailure('Microphone cannot restart with pending audio or a latched failure')
        self.api = self.api or backend()
        try:
            info = self.api.query_devices(self.device)
            host = self.api.query_hostapis(info['hostapi'])['name']
            if info['name'] != self.device_name or host != self.host_api or info['max_input_channels'] < 1:
                raise CaptureFailure('Selected microphone identity changed or is unavailable; list devices again')
            self.api.check_input_settings(device=self.device, samplerate=self.format.sample_rate,
                                          channels=1, dtype='int16')
            self._expected_stop = False
            self.stream = self.api.RawInputStream(device=self.device, samplerate=self.format.sample_rate,
                channels=1, dtype='int16', blocksize=self.block_frames, latency='high',
                callback=self._callback, finished_callback=self._finished)
            self.last_callback = time.monotonic()
            self.running = True
            self.stream.start()
            if self.error:
                raise CaptureFailure(self.error)
        except Exception as error:
            self.error = self.error or 'initialization_failed'
            self.stop()
            raise CaptureFailure('Selected microphone initialization failed: ' + str(error)) from error

    def failure(self):
        if self.error:
            return self.error
        try:
            if self.running and (not self.stream.active or time.monotonic() - self.last_callback > 2.0):
                self.error = 'device_disconnected'
        except Exception:
            self.error = 'device_status_failed'
        return self.error

    def read(self, frames):
        # Fixed callback blocks are at most the worker's requested block size.
        try:
            return self.blocks.popleft()
        except IndexError:
            return None

    def stop(self):
        self._expected_stop = True
        if self.stream is not None:
            try:
                self.stream.stop()  # Outside callback; supervisor bounds driver hangs.
            except Exception:
                self.error = self.error or 'stop_failed'
            finally:
                try:
                    self.stream.close()
                except Exception:
                    self.error = self.error or 'close_failed'
                self.stream = None
        self.running = False


def create_capture(config):
    if config.capture_source == 'fake':
        from .fake import FakeMicrophone
        return FakeMicrophone(AudioFormat(sample_rate=config.sample_rate), fail_at_frame=config.capture_fail_at)
    return Microphone(AudioFormat(sample_rate=config.sample_rate), device=config.input_device,
        device_name=config.input_device_name, host_api=config.input_host_api,
        block_frames=max(1, int(config.sample_rate * config.block_seconds)), buffer_blocks=config.queue_blocks)
