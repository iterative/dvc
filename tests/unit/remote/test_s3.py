from unittest import TestCase

from dvc.remote.s3 import RemoteS3


class TestRemoteS3(TestCase):
    bucket_name = "bucket-name"
    prefix = "some/prefix"
    url = "s3://{}/{}".format(bucket_name, prefix)

    def test_init(self):
        config = {"url": self.url}
        remote = RemoteS3(None, config)

        self.assertEqual(remote.path_info, self.url)

    def test_grants(self):
        config = {
            "url": self.url,
            "grant_read": "id=read-permission-id,id=other-read-permission-id",
            "grant_read_acp": "id=read-acp-permission-id",
            "grant_write_acp": "id=write-acp-permission-id",
            "grant_full_control": "id=full-control-permission-id",
        }
        remote = RemoteS3(None, config)

        self.assertEqual(
            remote.extra_args["GrantRead"],
            "id=read-permission-id,id=other-read-permission-id",
        )
        self.assertEqual(
            remote.extra_args["GrantReadACP"], "id=read-acp-permission-id"
        )
        self.assertEqual(
            remote.extra_args["GrantWriteACP"], "id=write-acp-permission-id"
        )
        self.assertEqual(
            remote.extra_args["GrantFullControl"],
            "id=full-control-permission-id",
        )
