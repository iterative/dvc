import getpass
import locale
import os
import platform
from contextlib import contextmanager
from subprocess import CalledProcessError, Popen, check_output

import pytest

from dvc.path_info import URLInfo

from .base import Base
from .local import Local


class HDFS(Base, URLInfo):
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

    @contextmanager
    def _hdfs(self):
        import pyarrow

        conn = pyarrow.hdfs.connect()
        try:
            yield conn
        finally:
            conn.close()

    def is_file(self):
        with self._hdfs() as _hdfs:
            return _hdfs.isfile(self.path)

    def is_dir(self):
        with self._hdfs() as _hdfs:
            return _hdfs.isfile(self.path)

    def exists(self):
        with self._hdfs() as _hdfs:
            return _hdfs.exists(self.path)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents
        assert not exist_ok

        with self._hdfs() as _hdfs:
            # NOTE: hdfs.mkdir always creates parents
            _hdfs.mkdir(self.path)

    def write_bytes(self, contents):
        with self._hdfs() as _hdfs:
            # NOTE: hdfs.open only supports 'rb', 'wb' or 'ab'
            with _hdfs.open(self.path, "wb") as fobj:
                fobj.write(contents)

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    def read_bytes(self):
        with self._hdfs() as _hdfs:
            # NOTE: hdfs.open only supports 'rb', 'wb' or 'ab'
            with _hdfs.open(self.path, "rb") as fobj:
                return fobj.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def hdfs():
    if not HDFS.should_test():
        pytest.skip("no hadoop running")
    yield HDFS(HDFS.get_url())
