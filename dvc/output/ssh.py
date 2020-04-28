from dvc.output.base import BaseOutput
from dvc.remote.ssh import SSHRemote


class SSHOutput(BaseOutput):
    REMOTE = SSHRemote
