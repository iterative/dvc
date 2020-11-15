from dvc.output.base import BaseOutput

from ..tree.gdrive import GDriveTree


class GDriveOutput(BaseOutput):
    TREE_CLS = GDriveTree
