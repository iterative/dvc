from unittest import TestCase

from dvc.remote.oss import RemoteOSS


class TestRemoteOSS(TestCase):
    bucket_name = "bucket-name"
    endpoint = "endpoint"
    key_id = "Fq2UVErCz4I6tq"
    key_secret = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"

    def test_init(self):
        prefix = "some/prefix"
        url = "oss://{}/{}".format(self.bucket_name, prefix)
        config = {
            "url": url,
            "oss_key_id": self.key_id,
            "oss_key_secret": self.key_secret,
            "oss_endpoint": self.endpoint,
        }
        remote = RemoteOSS(None, config)
        self.assertEqual(remote.url, url)
        self.assertEqual(remote.prefix, prefix)
        self.assertEqual(remote.bucket, self.bucket_name)
        self.assertEqual(remote.endpoint, self.endpoint)
        self.assertEqual(remote.key_id, self.key_id)
        self.assertEqual(remote.key_secret, self.key_secret)
