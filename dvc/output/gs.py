from dvc.output.base import BaseOutput

from ..tree.gs import GSTree


class GSOutput(BaseOutput):
    TREE_CLS = GSTree
