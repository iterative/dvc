import locale
import uuid
from contextlib import contextmanager

import pytest

from dvc.path_info import CloudURLInfo

from .base import Base


class WebHDFS(Base, CloudURLInfo):  # pylint: disable=abstract-method
    @contextmanager
    def _webhdfs(self):
        from hdfs import InsecureClient

        client = InsecureClient(
            f"http://{self.host}:{self.port}", self.user, root="/"
        )
        yield client

    def is_file(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path)["type"] == "FILE"

    def is_dir(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path)["type"] == "DIRECTORY"

    def exists(self):
        with self._webhdfs() as _hdfs:
            return _hdfs.status(self.path, strict=False) is not None

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

        with self._webhdfs() as _hdfs:
            # NOTE: hdfs.makekdirs always creates parents
            _hdfs.makedirs(self.path, permission=mode)

    def write_bytes(self, contents):
        with self._webhdfs() as _hdfs:
            with _hdfs.write(self.path, overwrite=True) as writer:
                writer.write(contents)

    def read_bytes(self):
        with self._webhdfs() as _hdfs:
            with _hdfs.read(self.path) as reader:
                return reader.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def webhdfs(hdfs_server):
    port = hdfs_server["webhdfs"]
    url = f"webhdfs://127.0.0.1:{port}/{uuid.uuid4()}"
    yield WebHDFS(url)
