import locale
import os
import platform
import subprocess
import uuid
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from dvc.path_info import URLInfo

from .base import Base

_hdfs_root = TemporaryDirectory()


class HDFS(Base, URLInfo):  # pylint: disable=abstract-method
    @contextmanager
    def _hdfs(self):
        import pyarrow.fs

        conn = pyarrow.fs.HadoopFileSystem(self.host, self.port)
        yield conn

    def is_file(self):
        with self._hdfs() as _hdfs:
            import pyarrow.fs

            file_info = _hdfs.get_file_info(self.path)
            return file_info.type == pyarrow.fs.FileType.File

    def is_dir(self):
        with self._hdfs() as _hdfs:
            import pyarrow.fs

            file_info = _hdfs.get_file_info(self.path)
            return file_info.type == pyarrow.fs.FileType.Directory

    def exists(self):
        with self._hdfs() as _hdfs:
            import pyarrow.fs

            file_info = _hdfs.get_file_info(self.path)
            return file_info.type != pyarrow.fs.FileType.NotFound

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

        with self._hdfs() as _hdfs:
            # NOTE: fs.create_dir creates parents by default
            _hdfs.create_dir(self.path)

    def write_bytes(self, contents):
        with self._hdfs() as _hdfs:
            with _hdfs.open_output_stream(self.path) as fobj:
                fobj.write(contents)

    def read_bytes(self):
        with self._hdfs() as _hdfs:
            with _hdfs.open_input_stream(self.path) as fobj:
                return fobj.read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture(scope="session")
def hadoop(test_config):
    test_config.requires("real_hdfs")
    pytest.importorskip("pyarrow.fs")
    if platform.system() != "Linux":
        pytest.skip("only supported on Linux")

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

    # NOTE: must set CLASSPATH to connect using pyarrow.fs.HadoopFileSystem
    result = subprocess.run(
        [f"{hadoop_home}/bin/hdfs", "classpath", "--glob"],
        universal_newlines=True,
        stdout=subprocess.PIPE,
        check=False,
    )
    os.environ["CLASSPATH"] = result.stdout


@pytest.fixture(scope="session")
def hdfs_server(hadoop, docker_compose, docker_services):
    import pyarrow.fs

    port = docker_services.port_for("hdfs", 8020)
    web_port = docker_services.port_for("hdfs", 50070)

    def _check():
        try:
            # NOTE: just connecting or even opening something is not enough,
            # we need to make sure that we are able to write something.
            conn = pyarrow.fs.HadoopFileSystem("hdfs://127.0.0.1", port)
            with conn.open_output_stream(str(uuid.uuid4())) as fobj:
                fobj.write(b"test")
            return True
        except (pyarrow.ArrowException, OSError):
            import traceback

            traceback.print_exc()
            return False

    try:
        docker_services.wait_until_responsive(
            timeout=30.0, pause=5, check=_check
        )
    except Exception:  # pylint: disable=broad-except
        pytest.skip("couldn't start hdfs server")

    return {"hdfs": port, "webhdfs": web_port}


@pytest.fixture
def real_hdfs(hdfs_server):
    port = hdfs_server["hdfs"]
    url = f"hdfs://127.0.0.1:{port}/{uuid.uuid4()}"
    yield HDFS(url)


def md5md5crc32c(path):
    # https://github.com/colinmarc/hdfs/blob/f2f512db170db82ad41590c4ba3b7718b13317d2/file_reader.go#L76
    import hashlib

    from crc32c import crc32c  # pylint: disable=no-name-in-module

    # dfs.bytes-per-checksum = 512, default on hadoop 2.7
    bytes_per_checksum = 512
    padded = 32
    total = 0

    md5md5 = hashlib.md5()

    with open(path, "rb") as fobj:
        while True:
            block = fobj.read(bytes_per_checksum)
            if not block:
                break

            crc_int = crc32c(block)

            # NOTE: hdfs is big-endian
            crc_bytes = crc_int.to_bytes(
                (crc_int.bit_length() + 7) // 8, "big"
            )

            md5 = hashlib.md5(crc_bytes).digest()

            total += len(md5)
            if padded < total:
                padded *= 2

            md5md5.update(md5)

    md5md5.update(b"\0" * (padded - total))
    return "000002000000000000000000" + md5md5.hexdigest()


def hadoop_fs_checksum(_, path_info):
    return md5md5crc32c(Path(_hdfs_root.name) / path_info.path.lstrip("/"))


class FakeHadoopFileSystem:
    def __init__(self, *args, **kwargs):
        from pyarrow.fs import LocalFileSystem

        self._root = Path(_hdfs_root.name)
        self._fs = LocalFileSystem()

    def _path(self, path):
        from pyarrow.fs import FileSelector

        if isinstance(path, FileSelector):
            return FileSelector(
                os.fspath(self._root / path.base_dir.lstrip("/")),
                path.allow_not_found,
                path.recursive,
            )
        if isinstance(path, list):
            return [self._path(sub_path) for sub_path in path]

        return os.fspath(self._root / path.lstrip("/"))

    def create_dir(self, path, **kwargs):
        return self._fs.create_dir(self._path(path), **kwargs)

    def open_input_stream(self, path, **kwargs):
        return self._fs.open_input_stream(self._path(path), **kwargs)

    def open_output_stream(self, path, **kwargs):
        import posixpath

        # NOTE: HadoopFileSystem.open_output_stream creates directories
        # automatically.
        self.create_dir(posixpath.dirname(path))
        return self._fs.open_output_stream(self._path(path), **kwargs)

    def get_file_info(self, path, **kwargs):
        from pyarrow.fs import FileInfo

        entries = self._fs.get_file_info(self._path(path), **kwargs)
        if isinstance(entries, FileInfo):
            return self._adjust_entry(entries)

        assert isinstance(entries, list)
        return list(map(self._adjust_entry, entries))

    def _adjust_entry(self, entry):
        import posixpath

        from pyarrow.fs import FileInfo

        mocked_path = os.path.relpath(entry.path, self._root)
        mocked_parts = mocked_path.split(os.path.sep)
        return FileInfo(
            path=posixpath.join(*mocked_parts),
            type=entry.type,
            mtime=entry.mtime,
            size=entry.size,
        )

    def move(self, from_path, to_path):
        self._fs.move(self._path(from_path), self._path(to_path))

    def delete_file(self, path):
        self._fs.delete_file(self._path(path))


@pytest.fixture
def hdfs(test_config, mocker):
    # Windows might not have Visual C++ Redistributable for Visual Studio
    # 2015 installed, which will result in the following error:
    # "The pyarrow installation is not built with support for
    # 'HadoopFileSystem'"
    test_config.requires("hdfs")
    pytest.importorskip("pyarrow.fs")

    mocker.patch("pyarrow.fs._not_imported", [])
    mocker.patch(
        "pyarrow.fs.HadoopFileSystem", FakeHadoopFileSystem, create=True
    )

    mocker.patch("dvc.fs.hdfs.HDFSFileSystem._checksum", hadoop_fs_checksum)

    url = f"hdfs://example.com:12345/{uuid.uuid4()}"
    yield HDFS(url)
