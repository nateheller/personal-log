"""Small durable I/O boundary, also used for deterministic fault injection."""

from dataclasses import dataclass
from hashlib import sha256
import os
from pathlib import Path, PurePosixPath
import struct
import wave

from .paths import UnsafeDataPath


def artifact_path(root: Path, relative: str) -> Path:
    parts = PurePosixPath(relative).parts
    if (not parts or relative != "/".join(parts) or any(p in {".", ".."} for p in parts)
            or "\\" in relative or ":" in relative or relative.startswith("/")):
        raise UnsafeDataPath("Invalid artifact reference")
    result = root.joinpath(*parts)
    if result.resolve() != result or not result.is_relative_to(root):
        raise UnsafeDataPath("Redirected artifact path")
    return result


def wav_header(frames: int, sample_rate: int) -> bytes:
    size = frames * 2
    if not 0 < size <= 0xFFFFFFFF - 36:
        raise ValueError("PCM segment is empty or exceeds RIFF limits")
    return struct.pack("<4sI4s4sIHHIIHH4sI", b"RIFF", 36 + size, b"WAVE", b"fmt ",
                       16, 1, 1, sample_rate, sample_rate * 2, 2, 16, b"data", size)


@dataclass(frozen=True)
class WavInfo:
    frames: int
    byte_length: int
    sha256: str


def inspect_wav(path: Path, sample_rate: int) -> WavInfo:
    """Only our canonical mono PCM16 WAV format is accepted; stream validation."""
    digest = sha256()
    with path.open("rb") as source:
        with wave.open(source, "rb") as audio:
            frames = audio.getnframes()
            if (audio.getnchannels(), audio.getsampwidth(), audio.getframerate(), audio.getcomptype()) != (1, 2, sample_rate, "NONE"):
                raise ValueError("Unexpected WAV format")
        source.seek(0)
        header = source.read(44)
        if header != wav_header(frames, sample_rate):
            raise ValueError("Unexpected WAV header")
        digest.update(header)
        size = 44
        while chunk := source.read(65536):
            digest.update(chunk)
            size += len(chunk)
        if size != 44 + frames * 2:
            raise ValueError("Truncated or extended WAV")
    return WavInfo(frames, size, digest.hexdigest())


def valid_wav(root: Path, relative: str, sample_rate: int, expected: WavInfo) -> bool:
    """Validate an effective reference without trusting cached availability.

    Missing, unreadable, redirected and malformed artifacts are all unusable.
    This performs no writes and does not alter historical recovery findings.
    """
    try:
        return inspect_wav(artifact_path(root, relative), sample_rate) == expected
    except (OSError, ValueError, EOFError, wave.Error):
        return False


class DurableIO:
    """Override methods or hook to inject partial writes, errors, or process exit."""

    def hook(self, boundary: str) -> None:
        pass

    def write(self, fd: int, data: memoryview) -> int:
        return os.write(fd, data)

    def write_all(self, fd: int, data: bytes) -> None:
        remaining = memoryview(data)
        while remaining:
            count = self.write(fd, remaining)
            if not 0 < count <= len(remaining):
                raise OSError("Invalid or zero-length write")
            remaining = remaining[count:]

    def flush(self, fd: int) -> None:
        os.fsync(fd)

    def sync_directory(self, path: Path) -> None:
        if os.name != "nt":
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def publish(self, source: Path, target: Path) -> None:
        if source.parent != target.parent:
            raise ValueError("Publication must stay within one directory")
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
            move.restype = wintypes.BOOL
            # WRITE_THROUGH, without REPLACE_EXISTING or COPY_ALLOWED.
            if not move(str(source), str(target), 0x8):
                raise ctypes.WinError(ctypes.get_last_error())
        else:
            os.link(source, target)  # Atomic no-clobber publication on POSIX.
            source.unlink()  # Only the successful builder's temporary link.
        self.sync_published(target)

    def sync_published(self, target: Path) -> None:
        fd = os.open(target, os.O_RDWR)
        try:
            self.flush(fd)
        finally:
            os.close(fd)
        self.sync_directory(target.parent)


class StoreLock:
    """OS-owned nonblocking lock; automatically released by process termination."""

    def __init__(self, path: Path):
        self.file = path.open("a+b", buffering=0)
        try:
            if os.name == "nt":
                import msvcrt
                if path.stat().st_size == 0:
                    self.file.write(b"\0")
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.file.close()
            raise

    def close(self):
        self.file.close()
