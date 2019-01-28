from __future__ import unicode_literals

from dvc.remote.gs import RemoteGS
from dvc.output.s3 import OutputS3


class OutputGS(OutputS3):
    REMOTE = RemoteGS
