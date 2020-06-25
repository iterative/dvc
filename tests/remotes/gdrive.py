import os
import uuid

import pytest
from funcy import cached_property

from dvc.remote.gdrive import GDriveRemoteTree

from .base import Base

TEST_GDRIVE_REPO_BUCKET = "root"


class GDrive(Base):
    @staticmethod
    def should_test():
        return os.getenv(GDriveRemoteTree.GDRIVE_CREDENTIALS_DATA) is not None

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "gdrive_service_account_email": "test",
            "gdrive_service_account_p12_file_path": "test.p12",
            "gdrive_use_service_account": True,
        }

    def __init__(self, dvc):
        tree = GDriveRemoteTree(dvc, self.config)
        tree._gdrive_create_dir("root", tree.path_info.path)

    @staticmethod
    def _get_storagepath():
        return TEST_GDRIVE_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        # NOTE: `get_url` should always return new random url
        return "gdrive://" + GDrive._get_storagepath()


@pytest.fixture
def gdrive(make_tmp_dir):
    if not GDrive.should_test():
        pytest.skip("no gdrive")

    # NOTE: temporary workaround
    tmp_dir = make_tmp_dir("gdrive", dvc=True)
    return GDrive(tmp_dir.dvc)


@pytest.fixture
def gdrive_remote(tmp_dir, dvc, gdrive):
    tmp_dir.add_remote(config=gdrive.config)
    return gdrive
