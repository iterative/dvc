from unittest import TestCase
import os

from dvc.remote.azure import RemoteAZURE
from dvc.exceptions import DvcException


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

        self.assertEqual(remote.path_info, "azure://" + self.container_name)
        self.assertEqual(remote.connection_string, self.connection_string)

    def test_init(self):
        prefix = "some/prefix"
        url = "azure://{}/{}".format(self.container_name, prefix)
        config = {"url": url, "connection_string": self.connection_string}
        remote = RemoteAZURE(None, config)

        self.assertEqual(remote.path_info, url)
        self.assertEqual(remote.connection_string, self.connection_string)

    def test_init_from_env(self):
        prefix = "some/prefix"
        url = "azure://{}/{}".format(self.container_name, prefix)
        env_var_name = "AZURE_TEST_VAR"
        os.environ[env_var_name] = self.connection_string

        config = {"url": url, "connection_string": "${}".format(env_var_name)}
        remote = RemoteAZURE(None, config)

        self.assertEqual(remote.path_info, url)
        self.assertEqual(remote.connection_string, self.connection_string)

        # clean  var from env again
        del os.environ[env_var_name]

    def test_init_from_env_not_set(self):
        prefix = "some/prefix"
        url = "azure://{}/{}".format(self.container_name, prefix)
        env_var_name = "AZURE_TEST_VAR"
        config = {"url": url, "connection_string": "${}".format(env_var_name)}

        with self.assertRaises(DvcException):
            RemoteAZURE(None, config)
