# pylint:disable=abstract-method
import os
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo
from dvc.tree.dropbox import REFRESH_TOKEN, FileCredProvider

from .base import Base

TEST_DBX_REPO_BUCKET = "dvc/tests-root"


class Dropbox(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        return bool(os.getenv(REFRESH_TOKEN)) or bool(
            os.getenv(FileCredProvider.DROPBOX_CREDENTIALS_FILE)
        )

    @cached_property
    def config(self):
        return {
            "url": self.url,
        }

    @staticmethod
    def get_url():
        # NOTE: `get_url` should always return new random url
        return "dropbox://" + TEST_DBX_REPO_BUCKET + "/" + str(uuid.uuid4())


@pytest.fixture
def dropbox(make_tmp_dir):
    if not Dropbox.should_test():
        pytest.skip("no dropbox")
    ret = Dropbox(Dropbox.get_url())
    return ret
