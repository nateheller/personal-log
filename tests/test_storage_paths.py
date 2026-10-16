import os
from pathlib import Path
import subprocess

import pytest

from personal_log.storage.database import open_database
from personal_log.storage.paths import UnsafeDataPath, data_file, prepare_data_root, validate_data_root


def test_default_external_directory(external, checkout, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(external))
    root = prepare_data_root(repository=checkout)
    assert root == external / "PersonalLog" / "DisposableCapture"
    assert root.is_dir()


@pytest.mark.parametrize("value", [None, "relative"])
def test_missing_or_relative_default_fails(checkout, monkeypatch, value):
    if value is None:
        monkeypatch.delenv("LOCALAPPDATA", raising=False)
    else:
        monkeypatch.setenv("LOCALAPPDATA", value)
    with pytest.raises(UnsafeDataPath):
        prepare_data_root(repository=checkout)


def test_reject_checkout_and_ancestors(checkout):
    for root in (checkout, checkout / "recordings", checkout.parent):
        with pytest.raises(UnsafeDataPath):
            prepare_data_root(repository=checkout, root=root)


def test_reject_relative_path(checkout):
    with pytest.raises(UnsafeDataPath):
        prepare_data_root(repository=checkout, root=Path("relative"))


@pytest.mark.parametrize("name", ["", ".", "..", "../x", "a/b", "a\\b", "x:stream"])
def test_reject_child_traversal(external, name):
    with pytest.raises(UnsafeDataPath):
        data_file(external, name)


def directory_alias(alias, target):
    if os.name == "nt":
        # Windows junctions do not require developer mode or symlink privileges.
        result = subprocess.run(["cmd", "/c", "mklink", "/J", str(alias), str(target)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    else:
        alias.symlink_to(target, target_is_directory=True)


def test_junction_into_checkout_rejected(external, checkout):
    alias = external / "alias"
    directory_alias(alias, checkout)
    with pytest.raises(UnsafeDataPath):
        prepare_data_root(repository=checkout, root=alias / "recordings")


def test_redirected_parent_rejected(external, checkout):
    alias = external / "alias"
    directory_alias(alias, checkout.parent)
    with pytest.raises(UnsafeDataPath):
        validate_data_root(alias / checkout.name / "audio", repository=checkout)


def test_regular_file_not_a_data_root(external, checkout):
    path = external / "file"
    path.write_text("synthetic")
    with pytest.raises(UnsafeDataPath):
        prepare_data_root(repository=checkout, root=path)


def test_database_sidecar_alias_rejected(external, checkout):
    root = external / "data"
    root.mkdir()
    directory_alias(root / "recording.sqlite3-wal", checkout)
    with pytest.raises(UnsafeDataPath):
        open_database(repository=checkout, root=root)
    assert not (root / "recording.sqlite3").exists()
