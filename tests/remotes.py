from .test_data_cloud import _should_test_aws
from .test_data_cloud import _should_test_azure
from .test_data_cloud import _should_test_gcp
from .test_data_cloud import _should_test_hdfs
from .test_data_cloud import _should_test_oss
from .test_data_cloud import _should_test_ssh
from .test_data_cloud import get_aws_url
from .test_data_cloud import get_azure_url
from .test_data_cloud import get_gcp_url
from .test_data_cloud import get_hdfs_url
from .test_data_cloud import get_local_url
from .test_data_cloud import get_oss_url
from .test_data_cloud import get_ssh_url

from dvc.remote.gs import RemoteGS
from dvc.remote.s3 import RemoteS3

from contextlib import contextmanager

from moto.s3 import mock_s3


# NOTE: staticmethod is only needed in Python 2
class Local:
    should_test = staticmethod(lambda: True)
    get_url = staticmethod(get_local_url)


class S3:
    should_test = staticmethod(_should_test_aws)
    get_url = staticmethod(get_aws_url)


class S3Mocked:
    should_test = staticmethod(lambda: True)
    get_url = staticmethod(get_aws_url)

    @classmethod
    def remote(cls):
        @contextmanager
        def inner():
            with mock_s3():
                remote = RemoteS3(None, {"url": cls.get_url()})
                yield remote

        return inner()

    @classmethod
    def put_objects(cls, remote, objects):
        @contextmanager
        def inner():
            s3 = cls.get_client(remote)
            s3.create_bucket(Bucket="dvc-test")
            for key, body in objects.items():
                cls.put_object(remote, key, body)
            yield

        return inner()

    @staticmethod
    def get_client(remote):
        return remote.s3

    @classmethod
    def put_object(cls, remote, key, body):
        s3 = cls.get_client(remote)
        bucket = remote.path_info.bucket

        s3.put_object(
            Bucket=bucket, Key=remote.path_info.path + "/" + key, Body=body
        )


class GCP:
    should_test = staticmethod(_should_test_gcp)
    get_url = staticmethod(get_gcp_url)

    @classmethod
    def remote(cls):
        @contextmanager
        def inner():
            remote = RemoteGS(None, {"url": cls.get_url()})
            yield remote

        return inner()

    @classmethod
    def put_objects(cls, remote, objects):
        @contextmanager
        def inner():
            for key, body in objects.items():
                cls.put_object(remote, key, body)
            yield
            cls.remove(remote, objects.keys())

        return inner()

    @classmethod
    def put_object(cls, remote, key, body):
        client = cls.get_client(remote)
        bucket = remote.path_info.bucket

        bucket = client.get_bucket(bucket)
        blob = bucket.blob(remote.path_info.path + "/" + key)
        blob.upload_from_string(body)

    @staticmethod
    def get_client(remote):
        return remote.gs

    @staticmethod
    def remove(remote, files):
        for fname in files:
            remote.remove(remote.path_info / fname)


class Azure:
    should_test = staticmethod(_should_test_azure)
    get_url = staticmethod(get_azure_url)


class OSS:
    should_test = staticmethod(_should_test_oss)
    get_url = staticmethod(get_oss_url)


class SSH:
    should_test = staticmethod(_should_test_ssh)
    get_url = staticmethod(get_ssh_url)


class HDFS:
    should_test = staticmethod(_should_test_hdfs)
    get_url = staticmethod(get_hdfs_url)


remote_params = [S3, GCP, Azure, OSS, SSH, HDFS]
all_remote_params = [Local] + remote_params
