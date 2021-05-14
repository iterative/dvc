from dvc.output.base import BaseOutput

from ..fs.ssh import SSHFileSystem


class SSHOutput(BaseOutput):
    FS_CLS = SSHFileSystem
