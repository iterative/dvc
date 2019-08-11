import os

import mock
import pytest

import dvc
from dvc.exceptions import DvcException
from dvc.remote.local import RemoteLOCAL
from tests.utils import trees_equal


class TestUnpackedDirStatusOptimization(object):
    @pytest.fixture(autouse=True)
    def setUp(self, dvc_repo, repo_dir):
        stages = dvc_repo.add(repo_dir.DATA_DIR)
        assert len(stages) == 1
        self.dir_out = stages[0].outs[0]
        self.unpacked_dir = (
            self.dir_out.cache_path + RemoteLOCAL.UNPACKED_DIR_SUFFIX
        )

    def test_should_not_fail_add_on_unpacked_dir_creation_exception(
        self, dvc_repo
    ):
        with mock.patch.object(
            dvc.remote.local.RemoteLOCAL,
            "_create_unpacked_dir",
            side_effect=self._raise_dvc_exception,
        ) as unpacked_create_spy, dvc_repo.state:
            assert not dvc_repo.cache.local.changed_cache(
                self.dir_out.checksum
            )
        assert unpacked_create_spy.call_count == 1

    def test_should_remove_unpacked_dir_on_create_exception(self, dvc_repo):
        # artificial unpacked dir for test purpose
        os.makedirs(self.unpacked_dir)
        assert os.path.exists(self.unpacked_dir)

        with mock.patch.object(
            dvc.remote.local.RemoteLOCAL,
            "_create_unpacked_dir",
            side_effect=self._raise_dvc_exception,
        ), dvc_repo.state:
            assert not dvc_repo.cache.local.changed_cache(
                self.dir_out.checksum
            )

        assert not os.path.exists(self.unpacked_dir)

    def test_should_create_unpacked_dir_on_status_check(
        self, dvc_repo, repo_dir
    ):
        assert not os.path.exists(self.unpacked_dir)

        with dvc_repo.state:
            assert not dvc_repo.cache.local.changed_cache(
                self.dir_out.checksum
            )

        assert os.path.exists(self.unpacked_dir)

        trees_equal(repo_dir.DATA_DIR, self.unpacked_dir)

    def test_should_proceed_with_full_check_on_modified_cache(self, dvc_repo):

        some_file_md5 = self.dir_out.dir_cache[0]["md5"]
        assert not os.path.exists(self.unpacked_dir)

        with dvc_repo.state:
            assert not dvc_repo.cache.local.changed_cache(
                self.dir_out.checksum
            )
        assert os.path.exists(self.unpacked_dir)

        with open(dvc_repo.cache.local.get(some_file_md5), "a") as fobj:
            fobj.write("string")

        with dvc_repo.state:
            assert dvc_repo.cache.local.changed_cache(self.dir_out.checksum)

    @staticmethod
    def _raise_dvc_exception(*args):
        raise DvcException("msg")
