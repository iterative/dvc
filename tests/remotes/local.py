# pylint: disable=cyclic-import,disable=abstract-method
import pytest

from tests.basic_env import TestDvc

from .base import Base


class Local(Base):
    @staticmethod
    def get_storagepath():
        return TestDvc.mkdtemp()

    @staticmethod
    def get_url():
        return Local.get_storagepath()


@pytest.fixture
def make_local(make_tmp_dir):
    def _make_local():
        ret = make_tmp_dir("local-cloud")
        ret.url = str(ret)
        ret.config = {"url": ret.url}
        return ret

    return _make_local


@pytest.fixture
def local_cloud(make_local):
    return make_local()


@pytest.fixture
def local_remote(tmp_dir, dvc, local_cloud):
    tmp_dir.add_remote(config=local_cloud.config)
    yield local_cloud
