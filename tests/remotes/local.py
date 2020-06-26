# pylint: disable=cyclic-import
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
def local_cloud():
    yield Local()


@pytest.fixture
def local_remote(tmp_dir, dvc, local_cloud):
    tmp_dir.add_remote(config=local_cloud.config)
    yield local_cloud
