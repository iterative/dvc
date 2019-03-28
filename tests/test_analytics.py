import os
import mock
import requests

from dvc.main import main
from dvc.analytics import Analytics
from tests.basic_env import TestDvc, TestGit, TestDir


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
    @mock.patch(
        "dvc.analytics.Analytics._is_enabled_config", return_value=True
    )
    @mock.patch("requests.post")
    def test_send(self, mockpost, _):
        ret = main(["daemon", "analytics", Analytics().dump(), "-v"])
        self.assertEqual(ret, 0)

        self.assertTrue(mockpost.called)

    @mock.patch.object(os, "getenv", new=_clean_getenv)
    @mock.patch(
        "dvc.analytics.Analytics._is_enabled_config", return_value=True
    )
    @mock.patch.object(
        requests, "post", side_effect=requests.exceptions.RequestException()
    )
    def test_send_failed(self, mockpost, _):
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
