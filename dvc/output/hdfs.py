from dvc.output.base import BaseOutput

from ..tree.hdfs import HDFSRemoteTree


class HDFSOutput(BaseOutput):
    TREE_CLS = HDFSRemoteTree
