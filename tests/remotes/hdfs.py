import locale
import os
import platform
import uuid
from contextlib import contextmanager

import pytest

from dvc.path_info import URLInfo
from tests import PY39, PYARROW_NOT_AVAILABLE

from .base import Base


class HDFS(Base, URLInfo):  # pylint: disable=abstract-method
    @contextmanager
    def _hdfs(self):
        import pyarrow

        conn = pyarrow.hdfs.connect(self.host, self.port)
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


@pytest.fixture(scope="session")
def hadoop():
    if platform.system() != "Linux":
        pytest.skip("only supported on Linux")

    if PY39:
        pytest.skip(PYARROW_NOT_AVAILABLE)

    import wget
    from appdirs import user_cache_dir

    hadoop_name = "hadoop-2.7.2.tar.gz"
    java_name = "openjdk-7u75-b13-linux-x64-18_dec_2014.tar.gz"

    base_url = "https://s3-us-east-2.amazonaws.com/dvc-public/dvc-test/"
    hadoop_url = base_url + hadoop_name
    java_url = base_url + java_name

    (cache_dir,) = (user_cache_dir("dvc-test", "iterative"),)
    dname = os.path.join(cache_dir, "hdfs")

    java_tar = os.path.join(dname, java_name)
    hadoop_tar = os.path.join(dname, hadoop_name)

    java_home = os.path.join(dname, "java-se-7u75-ri")
    hadoop_home = os.path.join(dname, "hadoop-2.7.2")

    def _get(url, tar, target):
        if os.path.isdir(target):
            return

        if not os.path.exists(tar):
            wget.download(url, out=tar)
        assert os.system(f"tar -xvf {tar} -C {dname}") == 0
        assert os.path.isdir(target)

    os.makedirs(dname, exist_ok=True)
    _get(hadoop_url, hadoop_tar, hadoop_home)
    _get(java_url, java_tar, java_home)

    os.environ["JAVA_HOME"] = java_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += f":{hadoop_home}/bin:{hadoop_home}/sbin"


@pytest.fixture(scope="session")
def hdfs_server(hadoop, docker_compose, docker_services):
    import pyarrow

    port = docker_services.port_for("hdfs", 8020)
    web_port = docker_services.port_for("hdfs", 50070)

    def _check():
        try:
            # NOTE: just connecting or even opening something is not enough,
            # we need to make sure that we are able to write something.
            conn = pyarrow.hdfs.connect("127.0.0.1", port)
            try:
                with conn.open(str(uuid.uuid4()), "wb") as fobj:
                    fobj.write(b"test")
            finally:
                conn.close()
            return True
        except (pyarrow.ArrowException, OSError):
            return False

    docker_services.wait_until_responsive(timeout=30.0, pause=5, check=_check)

    return {"hdfs": port, "webhdfs": web_port}


@pytest.fixture
def hdfs(hdfs_server):
    port = hdfs_server["hdfs"]
    url = f"hdfs://127.0.0.1:{port}/{uuid.uuid4()}"
    yield HDFS(url)


class WebHDFS(Base, URLInfo):  # pylint: disable=abstract-method
    @contextmanager
    def _webhdfs(self):
        from hdfs import InsecureClient

        client = InsecureClient(f"http://{self.host}:{self.port}", self.user)
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
        assert not exist_ok

        with self._webhdfs() as _hdfs:
            # NOTE: hdfs.makekdirs always creates parents
            _hdfs.makedirs(self.path, permission=mode)

    def write_bytes(self, contents):
        with self._webhdfs() as _hdfs:
            with _hdfs.write(self.path, overwrite=True) as writer:
                writer.write(contents)

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

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
