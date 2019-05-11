from __future__ import unicode_literals

import os
import re
import getpass
import posixpath
import logging
from subprocess import Popen, PIPE

from dvc.config import Config
from dvc.path import Schemes
from dvc.path.hdfs import HDFSPathInfo
from dvc.remote.base import RemoteBase, RemoteCmdError
from dvc.utils import fix_env, tmp_fname


logger = logging.getLogger(__name__)


class RemoteHDFS(RemoteBase):
    scheme = Schemes.HDFS
    REGEX = r"^hdfs://((?P<user>.*)@)?.*$"
    PARAM_CHECKSUM = "checksum"

    def __init__(self, repo, config):
        super(RemoteHDFS, self).__init__(repo, config)
        self.url = config.get(Config.SECTION_REMOTE_URL, "/")
        self.prefix = self.url
        self.user = self.group("user")
        if not self.user:
            self.user = config.get(
                Config.SECTION_REMOTE_USER, getpass.getuser()
            )

        self.path_info = HDFSPathInfo(user=self.user)

    def hadoop_fs(self, cmd, user=None):
        cmd = "hadoop fs -" + cmd
        if user:
            cmd = "HADOOP_USER_NAME={} ".format(user) + cmd

        # NOTE: close_fds doesn't work with redirected stdin/stdout/stderr.
        # See https://github.com/iterative/dvc/issues/1197.
        close_fds = os.name != "nt"

        executable = os.getenv("SHELL") if os.name != "nt" else None
        p = Popen(
            cmd,
            shell=True,
            close_fds=close_fds,
            executable=executable,
            env=fix_env(os.environ),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
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
        regex = r".*\t.*\t(?P<checksum>.*)"
        stdout = self.hadoop_fs(
            "checksum {}".format(path_info.path), user=path_info.user
        )
        return self._group(regex, stdout, "checksum")

    def copy(self, from_info, to_info):
        dname = posixpath.dirname(to_info.path)
        self.hadoop_fs("mkdir -p {}".format(dname), user=to_info.user)
        self.hadoop_fs(
            "cp -f {} {}".format(from_info.path, to_info.path),
            user=to_info.user,
        )

    def rm(self, path_info):
        self.hadoop_fs("rm -f {}".format(path_info.path), user=path_info.user)

    def remove(self, path_info):
        if path_info.scheme != "hdfs":
            raise NotImplementedError

        assert path_info.path

        logger.debug("Removing {}".format(path_info.path))

        self.rm(path_info)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == "hdfs"

        try:
            self.hadoop_fs("test -e {}".format(path_info.path))
            return True
        except RemoteCmdError:
            return False

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info.scheme != "hdfs":
                raise NotImplementedError

            if from_info.scheme != "local":
                raise NotImplementedError

            self.hadoop_fs(
                "mkdir -p {}".format(posixpath.dirname(to_info.path)),
                user=to_info.user,
            )

            tmp_file = tmp_fname(to_info.path)

            self.hadoop_fs(
                "copyFromLocal {} {}".format(from_info.path, tmp_file),
                user=to_info.user,
            )

            self.hadoop_fs(
                "mv {} {}".format(tmp_file, to_info.path), user=to_info.user
            )

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != "hdfs":
                raise NotImplementedError

            if to_info.scheme == "hdfs":
                self.copy(from_info, to_info)
                continue

            if to_info.scheme != "local":
                raise NotImplementedError

            dname = os.path.dirname(to_info.path)
            if not os.path.exists(dname):
                os.makedirs(dname)

            tmp_file = tmp_fname(to_info.path)

            self.hadoop_fs(
                "copyToLocal {} {}".format(from_info.path, tmp_file),
                user=from_info.user,
            )

            os.rename(tmp_file, to_info.path)

    def list_cache_paths(self):
        try:
            self.hadoop_fs("test -e {}".format(self.prefix))
        except RemoteCmdError:
            return []

        stdout = self.hadoop_fs("ls -R {}".format(self.prefix))
        lines = stdout.split("\n")
        flist = []
        for line in lines:
            if not line.startswith("-"):
                continue
            flist.append(line.split()[-1])
        return flist
