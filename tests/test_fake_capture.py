import struct

import pytest

from personal_log.recording.fake import AudioFormat, CaptureFailure, FakeMicrophone


def test_reproducible_frames_independent_of_block_sizes():
    first, second = FakeMicrophone(), FakeMicrophone()
    first.start()
    second.start()
    a, b = first.read(100), first.read(200)
    assert (a.start_frame, b.start_frame, b.frames) == (0, 100, 200)
    assert a.pcm + b.pcm == second.read(300).pcm
    assert struct.unpack("<hhh", a.pcm[:6]) == (-32768, -32767, -32766)


def test_pause_resume_preserves_position():
    capture = FakeMicrophone()
    capture.start()
    capture.read(10)
    capture.stop()
    with pytest.raises(RuntimeError):
        capture.read(10)
    capture.start()
    assert capture.read(10).start_frame == 10


def test_deterministic_interruption_preserves_prefix():
    capture = FakeMicrophone(fail_at_frame=15)
    capture.start()
    assert capture.read(10).frames == 10
    assert capture.read(10).frames == 5
    with pytest.raises(CaptureFailure):
        capture.read(10)
    assert not capture.running
    assert capture.position == 15


@pytest.mark.parametrize("frames", [0, -1, True, 1.2, 48_001])
def test_bounded_reads(frames):
    capture = FakeMicrophone()
    capture.start()
    with pytest.raises(ValueError):
        capture.read(frames)


@pytest.mark.parametrize("kwargs", [{"sample_rate": 0}, {"sample_rate": True}, {"channels": 2}, {"channels": True}, {"sample_width": 4}, {"sample_width": 2.0}])
def test_unsupported_audio_format(kwargs):
    with pytest.raises(ValueError):
        AudioFormat(**kwargs)
