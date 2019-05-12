from unittest import TestCase

from dvc.remote.azure import RemoteAZURE


class TestRemoteAZURE(TestCase):
    container_name = "container-name"
    connection_string = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
        "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )

    def test_init_compat(self):
        url = (
            "azure://ContainerName={container_name};{connection_string}"
        ).format(
            container_name=self.container_name,
            connection_string=self.connection_string,
        )
        config = {"url": url}
        remote = RemoteAZURE(None, config)
        self.assertEqual(remote.url, url)
        self.assertEqual(remote.prefix, "")
        self.assertEqual(remote.bucket, self.container_name)
        self.assertEqual(remote.connection_string, self.connection_string)

    def test_init(self):
        prefix = "some/prefix"
        url = "azure://{}/{}".format(self.container_name, prefix)
        config = {"url": url, "connection_string": self.connection_string}
        remote = RemoteAZURE(None, config)
        self.assertEqual(remote.url, url)
        self.assertEqual(remote.prefix, prefix)
        self.assertEqual(remote.bucket, self.container_name)
        self.assertEqual(remote.connection_string, self.connection_string)
