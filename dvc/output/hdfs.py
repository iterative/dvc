from dvc.output.base import OutputBase
from dvc.remote.hdfs import RemoteHDFS


class OutputHDFS(OutputBase):
    REMOTE = RemoteHDFS
