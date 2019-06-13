import os
import mock
import pytest

from dvc.updater import Updater


@pytest.fixture
def updater(dvc_repo):
    return Updater(dvc_repo.dvc_dir)


def test_updater(updater):
    # NOTE: only test on travis CRON to avoid generating too much logs
    travis = os.getenv("TRAVIS") == "true"
    if not travis:
        return

    cron = os.getenv("TRAVIS_EVENT_TYPE") == "cron"
    if not cron:
        return

    env = os.environ.copy()
    if env.get("CI"):
        del env["CI"]

    with mock.patch.dict(os.environ, env):
        updater.check()
        updater.check()
        updater.check()


def test_check_version_newer(updater):
    updater.latest = "0.20.8"
    updater.current = "0.21.0"

    assert not updater._is_outdated()


def test_check_version_equal(updater):
    updater.latest = "0.20.8"
    updater.current = "0.20.8"

    assert not updater._is_outdated()


def test_check_version_outdated(updater):
    updater.latest = "0.21.0"
    updater.current = "0.20.8"

    assert updater._is_outdated()
