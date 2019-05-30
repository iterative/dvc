from __future__ import unicode_literals

from dvc.output.base import OutputBase
from dvc.remote.hdfs import RemoteHDFS


class OutputHDFS(OutputBase):
    REMOTE = RemoteHDFS
