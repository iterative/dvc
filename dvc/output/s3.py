from __future__ import unicode_literals

from dvc.output.base import OutputBase
from dvc.remote.s3 import RemoteS3


class OutputS3(OutputBase):
    REMOTE = RemoteS3
