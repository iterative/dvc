import mock
import pytest

import dvc.remote.slow_link_detection
from dvc.config import Config
from dvc.remote.slow_link_detection import slow_link_guard


@pytest.fixture(autouse=True)
def timeout_immediately(monkeypatch):
    monkeypatch.setattr(dvc.remote.slow_link_detection, "timeout_seconds", 0.0)


@pytest.fixture
def make_remote():
    def _make_remote(cache_type=None, should_warn=True):
        remote = mock.Mock()
        remote.repo.config.config.get.return_value = {
            Config.SECTION_CACHE_TYPE: cache_type,
            Config.SECTION_CACHE_SLOW_LINK_WARNING: should_warn,
        }
        return remote

    return _make_remote


def test_show_warning_once(caplog, make_remote):
    remote = make_remote()
    slow_link_guard(lambda x: None)(remote)
    slow_link_guard(lambda x: None)(remote)

    assert caplog.records[0].message == dvc.remote.slow_link_detection.message
    assert len(caplog.records) == 1


def test_dont_warn_when_cache_type_is_set(caplog, make_remote):
    remote = make_remote(cache_type="copy")
    slow_link_guard(lambda x: None)(remote)

    assert len(caplog.records) == 0


def test_dont_warn_when_warning_is_disabled(caplog, make_remote):
    remote = make_remote(should_warn=False)
    slow_link_guard(lambda x: None)(remote)

    assert len(caplog.records) == 0
