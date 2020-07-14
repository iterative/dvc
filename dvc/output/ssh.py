from dvc.output.base import BaseOutput
from dvc.remote.ssh import SSHRemoteTree


class SSHOutput(BaseOutput):
    TREE_CLS = SSHRemoteTree
