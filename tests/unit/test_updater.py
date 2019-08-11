import os
import json

from dvc import __version__
from dvc.updater import Updater


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


def test_fetch(dvc_repo, mocker):
    updater = Updater(dvc_repo.dvc_dir)
    assert not os.path.exists(updater.updater_file)

    mock_get = mocker.patch("requests.get", side_effect=mocked_requests_get)
    updater.fetch(detach=False)
    mock_get.assert_called_once_with(Updater.URL, timeout=Updater.TIMEOUT_GET)

    assert os.path.isfile(updater.updater_file)
    with open(updater.updater_file, "r") as fobj:
        info = json.load(fobj)
    assert info["version"] == __version__
