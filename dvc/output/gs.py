from dvc.output.s3 import S3Output

from ..tree.gs import GSRemoteTree


class GSOutput(S3Output):
    TREE_CLS = GSRemoteTree
