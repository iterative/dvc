from dvc.output.base import OutputBase
from dvc.remote.ssh import SSHRemote


class OutputSSH(OutputBase):
    REMOTE = SSHRemote
