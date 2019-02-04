import mock
from unittest import TestCase

from dvc.remote.gs import RemoteGS


class TestRemoteGS(TestCase):
    BUCKET = "bucket"
    PREFIX = "prefix"
    URL = "gs://{}/{}".format(BUCKET, PREFIX)
    CREDENTIALPATH = "/path/to/gcp_credentials.json"
    PROJECT = "PROJECT"
    CONFIG = {
        "projectname": PROJECT,
        "url": URL,
        "credentialpath": CREDENTIALPATH,
    }

    def test_init(self):
        remote = RemoteGS(None, self.CONFIG)
        self.assertEqual(remote.url, self.URL)
        self.assertEqual(remote.prefix, self.PREFIX)
        self.assertEqual(remote.bucket, self.BUCKET)
        self.assertEqual(remote.projectname, self.PROJECT)
        self.assertEqual(remote.credentialpath, self.CREDENTIALPATH)

    @mock.patch("google.cloud.storage.Client.from_service_account_json")
    def test_gs(self, mock_client):
        remote = RemoteGS(None, self.CONFIG)
        self.assertTrue(remote.credentialpath)
        remote.gs()
        mock_client.assert_called_once_with(self.CREDENTIALPATH)

    @mock.patch("google.cloud.storage.Client")
    def test_gs_no_credspath(self, mock_client):
        config = self.CONFIG.copy()
        del config["credentialpath"]
        remote = RemoteGS(None, config)
        remote.gs()
        mock_client.assert_called_with(self.PROJECT)
