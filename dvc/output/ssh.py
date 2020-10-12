from dvc.output.base import BaseOutput

from ..tree.ssh import SSHTree


class SSHOutput(BaseOutput):
    TREE_CLS = SSHTree
