from pathlib import Path

import pytest


@pytest.fixture
def checkout():
    return Path(__file__).resolve().parents[1]


@pytest.fixture
def external(tmp_path, checkout):
    assert not tmp_path.resolve().is_relative_to(checkout)
    return tmp_path
