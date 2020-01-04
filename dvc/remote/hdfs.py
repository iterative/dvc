import io
import logging
import os
import posixpath
import re
import subprocess
from collections import deque
from contextlib import closing, contextmanager
from urllib.parse import urlparse

from .base import RemoteBASE, RemoteCmdError
from .pool import get_connection
from dvc.config import Config
from dvc.scheme import Schemes
from dvc.utils import fix_env, tmp_fname

logger = logging.getLogger(__name__)


class RemoteHDFS(RemoteBASE):
    scheme = Schemes.HDFS
    REGEX = r"^hdfs://((?P<user>.*)@)?.*$"
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"pyarrow": "pyarrow"}

    def __init__(self, repo, config):
        super().__init__(repo, config)
        self.path_info = None
        url = config.get(Config.SECTION_REMOTE_URL)
        if not url:
            return

        parsed = urlparse(url)

        user = parsed.username or config.get(Config.SECTION_REMOTE_USER)

        self.path_info = self.path_cls.from_parts(
            scheme=self.scheme,
            host=parsed.hostname,
            user=user,
            port=parsed.port,
            path=parsed.path,
        )

    def hdfs(self, path_info):
        import pyarrow

        return get_connection(
            pyarrow.hdfs.connect,
            path_info.host,
            path_info.port,
            user=path_info.user,
        )

    def hadoop_fs(self, cmd, user=None):
        cmd = "hadoop fs -" + cmd
        if user:
            cmd = "HADOOP_USER_NAME={} ".format(user) + cmd

        # NOTE: close_fds doesn't work with redirected stdin/stdout/stderr.
        # See https://github.com/iterative/dvc/issues/1197.
        close_fds = os.name != "nt"

        executable = os.getenv("SHELL") if os.name != "nt" else None
        p = subprocess.Popen(
            cmd,
            shell=True,
            close_fds=close_fds,
            executable=executable,
            env=fix_env(os.environ),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            raise RemoteCmdError(self.scheme, cmd, p.returncode, err)
        return out.decode("utf-8")

    @staticmethod
    def _group(regex, s, gname):
        match = re.match(regex, s)
        assert match is not None
        return match.group(gname)

    def get_file_checksum(self, path_info):
        # NOTE: pyarrow doesn't support checksum, so we need to use hadoop
        regex = r".*\t.*\t(?P<checksum>.*)"
        stdout = self.hadoop_fs(
            "checksum {}".format(path_info.path), user=path_info.user
        )
        return self._group(regex, stdout, "checksum")

    def copy(self, from_info, to_info, **_kwargs):
        dname = posixpath.dirname(to_info.path)
        with self.hdfs(to_info) as hdfs:
            hdfs.mkdir(dname)
            # NOTE: this is how `hadoop fs -cp` works too: it copies through
            # your local machine.
            with hdfs.open(from_info.path, "rb") as from_fobj:
                tmp_info = to_info.parent / tmp_fname(to_info.name)
                try:
                    with hdfs.open(tmp_info.path, "wb") as tmp_fobj:
                        tmp_fobj.upload(from_fobj)
                    hdfs.rename(tmp_info.path, to_info.path)
                except Exception:
                    self.remove(tmp_info)
                    raise

    def remove(self, path_info):
        if path_info.scheme != "hdfs":
            raise NotImplementedError

        if self.exists(path_info):
            logger.debug("Removing {}".format(path_info.path))
            with self.hdfs(path_info) as hdfs:
                hdfs.rm(path_info.path)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == "hdfs"
        with self.hdfs(path_info) as hdfs:
            return hdfs.exists(path_info.path)

    def _upload(self, from_file, to_info, **_kwargs):
        with self.hdfs(to_info) as hdfs:
            hdfs.mkdir(posixpath.dirname(to_info.path))
            tmp_file = tmp_fname(to_info.path)
            with open(from_file, "rb") as fobj:
                hdfs.upload(tmp_file, fobj)
            hdfs.rename(tmp_file, to_info.path)

    def _download(self, from_info, to_file, **_kwargs):
        with self.hdfs(from_info) as hdfs:
            with open(to_file, "wb+") as fobj:
                hdfs.download(from_info.path, fobj)

    @contextmanager
    def open(self, path_info, mode="r", encoding=None):
        assert mode in {"r", "rt", "rb"}

        try:
            with self.hdfs(path_info) as hdfs, closing(
                hdfs.open(path_info.path, mode="rb")
            ) as fd:
                if mode == "rb":
                    yield fd
                else:
                    yield io.TextIOWrapper(fd, encoding=encoding)
        except IOError as e:
            # Empty .errno and not specific enough error class in pyarrow,
            # see https://issues.apache.org/jira/browse/ARROW-6248
            if "file does not exist" in str(e):
                raise FileNotFoundError(*e.args)
            raise

    def list_cache_paths(self):
        if not self.exists(self.path_info):
            return

        dirs = deque([self.path_info.path])

        with self.hdfs(self.path_info) as hdfs:
            while dirs:
                for entry in hdfs.ls(dirs.pop(), detail=True):
                    if entry["kind"] == "directory":
                        dirs.append(urlparse(entry["name"]).path)
                    elif entry["kind"] == "file":
                        yield urlparse(entry["name"]).path
