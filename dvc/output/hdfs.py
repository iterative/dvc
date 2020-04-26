from dvc.output.base import OutputBase
from dvc.remote.hdfs import HDFSRemote


class OutputHDFS(OutputBase):
    REMOTE = HDFSRemote
