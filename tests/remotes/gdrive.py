# pylint:disable=abstract-method
import os
import uuid
from functools import partial, partialmethod

import pytest
from funcy import cached_property

from dvc.fs.gdrive import GDriveFileSystem
from dvc.path_info import CloudURLInfo
from dvc.utils import tmp_fname

from .base import Base

TEST_GDRIVE_REPO_BUCKET = "root"

CREDENTIALS_DIR = os.path.join(os.path.expanduser("~"), ".config", "pydata")
CREDENTIALS_FILE = os.path.join(
    CREDENTIALS_DIR, "pydata_google_credentials.json"
)


class GDrive(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        return bool(os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA))

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "gdrive_service_account_email": "test",
            "gdrive_service_account_p12_file_path": "test.p12",
            "gdrive_use_service_account": True,
        }

    @staticmethod
    def _get_storagepath():
        return TEST_GDRIVE_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        # NOTE: `get_url` should always return new random url
        return "gdrive://" + GDrive._get_storagepath()

    @cached_property
    def client(self):
        import pydata_google_auth

        try:
            from gdrivefs import GoogleDriveFileSystem
        except ImportError:
            pytest.skip("gdrivefs is not installed")

        tmp_path = tmp_fname()
        with open(tmp_path, "w") as stream:
            stream.write(os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA))

        GoogleDriveFileSystem._connect_cache = partial(
            pydata_google_auth.load_user_credentials, tmp_path
        )
        return GoogleDriveFileSystem(token="cache")

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        if not self.client.exists(self.path):
            self.client.mkdir(self.path)

    def write_bytes(self, contents):
        with self.client.open(self.path, mode="wb") as stream:
            stream.write(contents)

    def _read(self, mode):
        with self.client.open(self.path, mode=mode) as stream:
            return stream.read()

    def cleanup(self):
        self.client.delete(self.path, recursive=True)

    read_text = partialmethod(_read, mode="r")
    read_bytes = partialmethod(_read, mode="rb")


@pytest.fixture
def gdrive(test_config, make_tmp_dir):
    pytest.skip("temporarily disabled")

    test_config.requires("gdrive")
    if not GDrive.should_test():
        pytest.skip("no gdrive")

    # NOTE: temporary workaround
    tmp_dir = make_tmp_dir("gdrive", dvc=True)

    ret = GDrive(GDrive.get_url())
    fs = GDriveFileSystem(tmp_dir.dvc, ret.config)
    fs._gdrive_create_dir("root", fs.path_info.path)
    yield ret
