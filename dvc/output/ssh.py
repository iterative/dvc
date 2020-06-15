from dvc.output.base import BaseOutput
from dvc.remote.ssh import SSHRemote, SSHRemoteTree


class SSHOutput(BaseOutput):
    REMOTE_CLS = SSHRemote
    TREE_CLS = SSHRemoteTree
