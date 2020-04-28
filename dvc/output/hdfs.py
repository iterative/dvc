from dvc.output.base import BaseOutput
from dvc.remote.hdfs import HDFSRemote


class HDFSOutput(BaseOutput):
    REMOTE = HDFSRemote
