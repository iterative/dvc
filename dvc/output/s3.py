from dvc.output.base import BaseOutput
from dvc.remote.s3 import S3Remote


class S3Output(BaseOutput):
    REMOTE = S3Remote
