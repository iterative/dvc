from dvc.output.s3 import S3Output

from ..tree.gs import GSTree


class GSOutput(S3Output):
    TREE_CLS = GSTree
