import os
import json
import mock

from dvc import __version__
from tests.basic_env import TestDvc


class MockResponse(object):
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse({"version": __version__}, 200)


class TestUpdater(TestDvc):
    @mock.patch("requests.get", side_effect=mocked_requests_get)
    def test_fetch(self, mock_get):
        self.assertFalse(os.path.exists(self.dvc.updater.updater_file))
        self.dvc.updater.fetch(detach=False)
        mock_get.assert_called_once()
        self.assertTrue(os.path.isfile(self.dvc.updater.updater_file))
        with open(self.dvc.updater.updater_file, "r") as fobj:
            info = json.load(fobj)
        self.assertEqual(info["version"], __version__)
