# pylint:disable=abstract-method
import os
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo
from dvc.tree.gdrive import GDriveTree

from .base import Base

TEST_GDRIVE_REPO_BUCKET = "root"


class GDrive(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        return bool(os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA))

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


@pytest.fixture
def gdrive(make_tmp_dir):
    if not GDrive.should_test():
        pytest.skip("no gdrive")

    # NOTE: temporary workaround
    tmp_dir = make_tmp_dir("gdrive", dvc=True)

    ret = GDrive(GDrive.get_url())
    tree = GDriveTree(tmp_dir.dvc, ret.config)
    tree._gdrive_create_dir("root", tree.path_info.path)
    return ret
