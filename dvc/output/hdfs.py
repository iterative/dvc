from dvc.output.base import BaseOutput
from dvc.remote.hdfs import HDFSRemoteTree


class HDFSOutput(BaseOutput):
    REMOTE = HDFSRemoteTree
