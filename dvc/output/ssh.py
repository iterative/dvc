from dvc.output.base import BaseOutput

from ..tree.ssh import SSHRemoteTree


class SSHOutput(BaseOutput):
    TREE_CLS = SSHRemoteTree
