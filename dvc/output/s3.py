from dvc.output.base import OutputBase
from dvc.remote.s3 import S3Remote


class OutputS3(OutputBase):
    REMOTE = S3Remote
