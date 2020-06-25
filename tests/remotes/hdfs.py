import getpass
import os
import platform
from subprocess import CalledProcessError, Popen, check_output

import pytest

from .base import Base
from .local import Local


class HDFS(Base):
    @staticmethod
    def should_test():
        if platform.system() != "Linux":
            return False

        try:
            # pylint: disable=unexpected-keyword-arg
            # see: https://github.com/PyCQA/pylint/issues/3645
            check_output(
                ["hadoop", "version"],
                shell=True,
                executable=os.getenv("SHELL"),
            )
        except (CalledProcessError, OSError):
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


@pytest.fixture
def hdfs():
    if not HDFS.should_test():
        pytest.skip("no hadoop running")
    yield HDFS()


@pytest.fixture
def hdfs_remote(tmp_dir, dvc, hdfs):
    tmp_dir.add_remote(config=hdfs.config)
    yield hdfs
