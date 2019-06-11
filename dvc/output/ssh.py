from __future__ import unicode_literals

from dvc.output.base import OutputBase
from dvc.remote.ssh import RemoteSSH


class OutputSSH(OutputBase):
    REMOTE = RemoteSSH
