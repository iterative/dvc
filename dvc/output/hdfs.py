from dvc.output.base import BaseOutput

from ..fs.hdfs import HDFSFileSystem


class HDFSOutput(BaseOutput):
    FS_CLS = HDFSFileSystem
