# pylint:disable=abstract-method
import locale
import os
import pathlib
import tempfile
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo, PathInfo
from dvc.tree.gdrive import GDriveTree, GDriveURLInfo

from .base import Base

TEST_GDRIVE_REPO_BUCKET = "root"


class GDrive(Base, CloudURLInfo):
    def __init__(self, url, dvc):
        CloudURLInfo.__init__(self, url)
        Base.__init__(self, url)
        self.tree = GDriveTree(dvc, self.config)

    @staticmethod
    def should_test():
        return bool(os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA))

    @cached_property
    def config(self):
        if GDrive.should_test():
            return {
                "url": self.url,
            }
        else:
            return {
                "url": self.url,
                "gdrive_service_account_email": "test",
                "gdrive_service_account_p12_file_path": "test.p12",
                "gdrive_use_service_account": True,
            }

    @staticmethod
    def _get_storagepath():
        return TEST_GDRIVE_REPO_BUCKET + "/" + str(uuid.uuid4())

    @cached_property
    def _get_gdrive_path_info(self):
        return GDriveURLInfo(self.url)

    def _join(self, name, prefix=None):
        path = Base._join(self, name, prefix)
        path.tree = self.tree
        return path

    @staticmethod
    def get_url():
        # NOTE: `get_url` should always return new random url
        return "gdrive://" + GDrive._get_storagepath()

    def is_file(self):
        return self.tree.isfile(self._get_gdrive_path_info)

    def is_dir(self):
        return self.tree.isdir(self._get_gdrive_path_info)

    def exists(self):
        return self.tree.exists(self._get_gdrive_path_info)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents
        assert not exist_ok

    def write_bytes(self, contents):
        f = tempfile.NamedTemporaryFile()
        f.write(contents)
        f.flush()

        local = PathInfo(pathlib.Path(f.name))
        self.tree.upload(local, self._get_gdrive_path_info)

        f.close()

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    # pylint: disable=E1101
    def read_bytes(self):
        stream = self.tree.open(self._get_gdrive_path_info, mode="rb")
        return stream.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def gdrive(make_tmp_dir):
    if not GDrive.should_test():
        pytest.skip("no gdrive")

    # NOTE: temporary workaround
    tmp_dir = make_tmp_dir("gdrive", dvc=True)

    ret = GDrive(GDrive.get_url(), tmp_dir.dvc)
    tree = ret.tree
    tree._gdrive_create_dir("root", tree.path_info.path)
    return ret
