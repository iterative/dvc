import os
import platform
import uuid
import getpass

from contextlib import contextmanager
from subprocess import CalledProcessError, check_output, Popen

from dvc.utils import env2bool
from dvc.remote import RemoteGDrive
from dvc.remote.gs import RemoteGS
from dvc.remote.s3 import RemoteS3
from tests.basic_env import TestDvc

from moto.s3 import mock_s3


TEST_REMOTE = "upstream"
TEST_CONFIG = {
    "cache": {},
    "core": {"remote": TEST_REMOTE},
    "remote": {TEST_REMOTE: {"url": ""}},
}

TEST_AWS_REPO_BUCKET = os.environ.get("DVC_TEST_AWS_REPO_BUCKET", "dvc-test")
TEST_GCP_REPO_BUCKET = os.environ.get("DVC_TEST_GCP_REPO_BUCKET", "dvc-test")
TEST_OSS_REPO_BUCKET = "dvc-test"

TEST_GCP_CREDS_FILE = os.path.abspath(
    os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join("scripts", "ci", "gcp-creds.json"),
    )
)
# Ensure that absolute path is used
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = TEST_GCP_CREDS_FILE

TEST_GDRIVE_CLIENT_ID = (
    "217948389181-rs7it4a635b3qrf8dnmklmoj2kimun9n.apps.googleusercontent.com"
)
TEST_GDRIVE_CLIENT_SECRET = "LNg9n_cK7bohI8gEHn4bUeMX"

always_test = staticmethod(lambda: True)


class Local:
    should_test = always_test

    @staticmethod
    def get_storagepath():
        return TestDvc.mkdtemp()

    @staticmethod
    def get_url():
        return Local.get_storagepath()


class S3:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_AWS", undefined=None)
        if do_test is not None:
            return do_test

        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        ):
            return True

        return False

    @staticmethod
    def get_storagepath():
        return TEST_AWS_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "s3://" + S3.get_storagepath()


class S3Mocked(S3):
    should_test = always_test

    @classmethod
    @contextmanager
    def remote(cls):
        with mock_s3():
            yield RemoteS3(None, {"url": cls.get_url()})

    @staticmethod
    def put_objects(remote, objects):
        s3 = remote.s3
        bucket = remote.path_info.bucket
        s3.create_bucket(Bucket=bucket)
        for key, body in objects.items():
            s3.put_object(
                Bucket=bucket, Key=(remote.path_info / key).path, Body=body
            )


class GCP:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_GCP", undefined=None)
        if do_test is not None:
            return do_test

        if not os.path.exists(TEST_GCP_CREDS_FILE):
            return False

        try:
            check_output(
                [
                    "gcloud",
                    "auth",
                    "activate-service-account",
                    "--key-file",
                    TEST_GCP_CREDS_FILE,
                ]
            )
        except (CalledProcessError, OSError):
            return False
        return True

    @staticmethod
    def get_storagepath():
        return TEST_GCP_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "gs://" + GCP.get_storagepath()

    @classmethod
    @contextmanager
    def remote(cls):
        yield RemoteGS(None, {"url": cls.get_url()})

    @staticmethod
    def put_objects(remote, objects):
        client = remote.gs
        bucket = client.get_bucket(remote.path_info.bucket)
        for key, body in objects.items():
            bucket.blob((remote.path_info / key).path).upload_from_string(body)


class GDrive:
    @staticmethod
    def should_test():
        return os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA) is not None

    def get_url(self):
        if not getattr(self, "_remote_url", None):
            self._remote_url = "gdrive://root/" + str(uuid.uuid4())
        return self._remote_url


class Azure:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_AZURE", undefined=None)
        if do_test is not None:
            return do_test

        return os.getenv("AZURE_STORAGE_CONTAINER_NAME") and os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )

    @staticmethod
    def get_url():
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        assert container_name is not None
        return "azure://{}/{}".format(container_name, str(uuid.uuid4()))


class OSS:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_OSS", undefined=None)
        if do_test is not None:
            return do_test

        return (
            os.getenv("OSS_ENDPOINT")
            and os.getenv("OSS_ACCESS_KEY_ID")
            and os.getenv("OSS_ACCESS_KEY_SECRET")
        )

    @staticmethod
    def get_storagepath():
        return "{}/{}".format(TEST_OSS_REPO_BUCKET, (uuid.uuid4()))

    @staticmethod
    def get_url():
        return "oss://{}".format(OSS.get_storagepath())


class SSH:
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_SSH", undefined=None)
        if do_test is not None:
            return do_test

        # FIXME: enable on windows
        if os.name == "nt":
            return False

        try:
            check_output(["ssh", "-o", "BatchMode=yes", "127.0.0.1", "ls"])
        except (CalledProcessError, IOError):
            return False

        return True

    @staticmethod
    def get_url():
        return "ssh://{}@127.0.0.1:22{}".format(
            getpass.getuser(), Local.get_storagepath()
        )


class SSHMocked:
    should_test = always_test

    @staticmethod
    def get_url(user, port):
        path = Local.get_storagepath()
        if os.name == "nt":
            # NOTE: On Windows Local.get_storagepath() will return an
            # ntpath that looks something like `C:\some\path`, which is not
            # compatible with SFTP paths [1], so we need to convert it to
            # a proper posixpath.
            # To do that, we should construct a posixpath that would be
            # relative to the server's root.
            # In our case our ssh server is running with `c:/` as a root,
            # and our URL format requires absolute paths, so the
            # resulting path would look like `/some/path`.
            #
            # [1]https://tools.ietf.org/html/draft-ietf-secsh-filexfer-13#section-6
            drive, path = os.path.splitdrive(path)
            assert drive.lower() == "c:"
            path = path.replace("\\", "/")
        url = "ssh://{}@127.0.0.1:{}{}".format(user, port, path)
        return url


class HDFS:
    @staticmethod
    def should_test():
        if platform.system() != "Linux":
            return False

        try:
            check_output(
                ["hadoop", "version"],
                shell=True,
                executable=os.getenv("SHELL"),
            )
        except (CalledProcessError, IOError):
            return False

        p = Popen(
            "hadoop fs -ls hdfs://127.0.0.1/",
            shell=True,
            executable=os.getenv("SHELL"),
        )
        p.communicate()
        if p.returncode != 0:
            return False

        return True

    @staticmethod
    def get_url():
        return "hdfs://{}@127.0.0.1{}".format(
            getpass.getuser(), Local.get_storagepath()
        )
