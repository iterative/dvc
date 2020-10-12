from dvc.output.base import BaseOutput

from ..tree.hdfs import HDFSTree


class HDFSOutput(BaseOutput):
    TREE_CLS = HDFSTree
