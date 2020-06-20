from dvc.output.base import BaseOutput
from dvc.remote.s3 import S3RemoteTree


class S3Output(BaseOutput):
    TREE_CLS = S3RemoteTree
