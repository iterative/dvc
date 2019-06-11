from __future__ import unicode_literals

from dvc.remote.s3 import RemoteS3
from dvc.output.base import OutputBase


class OutputS3(OutputBase):
    REMOTE = RemoteS3
