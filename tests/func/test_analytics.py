import os

import mock
import requests

from dvc.analytics import Analytics
from dvc.main import main
from tests.basic_env import TestDir
from tests.basic_env import TestDvc
from tests.basic_env import TestGit


def _clean_getenv(key, default=None):
    """
    Remove env vars that affect dvc behavior in tests
    """
    if key in ["DVC_TEST", "CI"]:
        return None
    return os.environ.get(key, default)


class TestAnalytics(TestDir):
    def test(self):
        a = Analytics()
        a.collect()
        self.assertTrue(isinstance(a.info, dict))
        self.assertNotEqual(a.info, {})
        self.assertTrue(a.PARAM_USER_ID in a.info.keys())
        self.assertTrue(a.PARAM_SYSTEM_INFO in a.info.keys())
        self.assertNotEqual(a.info[a.PARAM_SYSTEM_INFO], {})

    @mock.patch.object(os, "getenv", new=_clean_getenv)
    @mock.patch("requests.post")
    def test_send(self, mockpost):
        ret = main(["daemon", "analytics", Analytics().dump(), "-v"])
        self.assertEqual(ret, 0)

        self.assertTrue(mockpost.called)

    @mock.patch.object(os, "getenv", new=_clean_getenv)
    @mock.patch.object(
        requests, "post", side_effect=requests.exceptions.RequestException()
    )
    def test_send_failed(self, mockpost):
        ret = main(["daemon", "analytics", Analytics().dump(), "-v"])
        self.assertEqual(ret, 0)

        self.assertTrue(mockpost.called)


class TestAnalyticsGit(TestAnalytics, TestGit):
    pass


class TestAnalyticsDvc(TestAnalytics, TestDvc):
    @mock.patch("requests.post")
    def test_send_disabled(self, mockpost):
        ret = main(["config", "core.analytics", "false"])
        self.assertEqual(ret, 0)

        with mock.patch.object(os, "getenv", new=_clean_getenv):
            ret = main(["daemon", "analytics", Analytics().dump(), "-v"])
        self.assertEqual(ret, 0)

        self.assertFalse(mockpost.called)
