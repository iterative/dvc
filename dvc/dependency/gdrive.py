from dvc.dependency.base import BaseDependency
from dvc.output import BaseOutput
from dvc.tree import GDriveTree


class GDriveDependency(BaseDependency, BaseOutput):
    TREE_CLS = GDriveTree
