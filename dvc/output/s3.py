from dvc.output.base import BaseOutput

from ..tree.s3 import S3Tree


class S3Output(BaseOutput):
    TREE_CLS = S3Tree
