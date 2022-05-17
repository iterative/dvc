import os
import re
import subprocess
import sys
import threading

from funcy import cached_property, wrap_prop

from ..base import FileSystem

CHECKSUM_REGEX = re.compile(r".*\t.*\t(?P<checksum>.*)")


def fix_env():
    env = os.environ.copy()
    if getattr(sys, "frozen", False):
        lp_key = "LD_LIBRARY_PATH"
        lp_orig = env.get(lp_key + "_ORIG", None)
        if lp_orig is not None:
            env[lp_key] = lp_orig
        else:
            env.pop(lp_key, None)
    return env


# pylint: disable=abstract-method
class HDFSFileSystem(FileSystem):
    protocol = "hdfs"
    REQUIRES = {"fsspec": "fsspec", "pyarrow": "pyarrow"}
    PARAM_CHECKSUM = "checksum"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        host = self.fs_args["host"]
        port = self.fs_args.get("port")
        netloc = host + (f":{port}" if port else "")
        return "hdfs://" + netloc + "/" + path.lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.arrow import HadoopFileSystem

        # pylint:disable=protected-access
        return HadoopFileSystem._get_kwargs_from_urls(urlpath)

    def _prepare_credentials(self, **config):
        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from fsspec.implementations.arrow import HadoopFileSystem

        return HadoopFileSystem(**self.fs_args)

    def checksum(self, path):
        return self._checksum(path)

    def _checksum(self, path, **kwargs):
        # PyArrow doesn't natively support retrieving the
        # checksum, so we have to use hadoop fs

        url = self.unstrip_protocol(path)

        result = self._run_command(
            f"checksum {url}",
            env=fix_env(),
            user=self.fs_args.get("user"),
        )
        if result is None:
            return None

        match = CHECKSUM_REGEX.match(result)
        if match is None:
            return None

        return match.group("checksum")

    def _run_command(self, cmd, env=None, user=None):
        cmd = "hadoop fs -" + cmd
        if user:
            cmd = f"HADOOP_USER_NAME={user} " + cmd

        # NOTE: close_fds doesn't work with redirected stdin/stdout/stderr.
        # See https://github.com/iterative/dvc/issues/1197.
        close_fds = os.name != "nt"

        executable = os.getenv("SHELL") if os.name != "nt" else None
        p = subprocess.Popen(
            cmd,
            shell=True,
            close_fds=close_fds,
            executable=executable,
            env=env or os.environ,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = p.communicate()

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd, out, err)
        else:
            return out.decode("utf-8")
