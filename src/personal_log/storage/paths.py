"""Fail-closed external data paths, including existing Windows junctions."""

import os
from pathlib import Path


class UnsafeDataPath(ValueError):
    pass


def validate_data_root(root: Path, *, repository: Path) -> Path:
    root, repository = Path(root), Path(repository)
    if not root.is_absolute() or not repository.is_absolute():
        raise UnsafeDataPath("Data root and repository must be absolute")
    resolved, checkout = root.resolve(), repository.resolve(strict=True)
    if resolved.is_relative_to(checkout) or checkout.is_relative_to(resolved):
        raise UnsafeDataPath("Application data must not overlap the Git checkout")
    # Local volumes only. UNC shares do not provide the assumed SQLite semantics.
    if os.name == "nt" and resolved.drive.startswith("\\\\"):
        raise UnsafeDataPath("Network storage is unsupported")
    if resolved.exists() and not resolved.is_dir():
        raise UnsafeDataPath("Application data root must be a directory")
    return resolved


def prepare_data_root(*, repository: Path, root: Path | None = None) -> Path:
    if root is None:
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise UnsafeDataPath("LOCALAPPDATA is unavailable; no working-directory fallback")
        local_path = Path(local)
        if not local_path.is_absolute():
            raise UnsafeDataPath("LOCALAPPDATA must be absolute")
        root = local_path / "PersonalLog" / "DisposableCapture"
    safe = validate_data_root(root, repository=repository)
    safe.mkdir(parents=True, exist_ok=True)
    # Recheck the original spelling in case an existing ancestor was redirected.
    if validate_data_root(root, repository=repository) != safe:
        raise UnsafeDataPath("Application data path changed during creation")
    return safe


def data_file(root: Path, name: str) -> Path:
    """Resolve a direct child without following it outside the validated root."""
    if not name or name in {".", ".."} or "/" in name or "\\" in name or ":" in name:
        raise UnsafeDataPath("Expected a plain file name")
    root = root.resolve(strict=True)
    candidate = root / name
    if candidate.resolve() != candidate:
        raise UnsafeDataPath("Data file must not be a symlink or junction")
    return candidate
