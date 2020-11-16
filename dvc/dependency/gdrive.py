from dvc.dependency.base import BaseDependency
from dvc.output import BaseOutput
from dvc.tree import GDriveTree
from dvc.tree.gdrive import GDriveURLInfo


class GDriveDependency(BaseDependency, BaseOutput):
    TREE_CLS = GDriveTree

    def __init__(self, stage, path, info=None, **wkwargs):
        repo = stage.repo if stage else None
        if "tree" not in wkwargs:
            base_path = GDriveURLInfo(path).replace(path="").url
            tree = GDriveTree(repo, {"url": base_path})
            wkwargs["tree"] = tree
        BaseOutput.__init__(self, stage, path, info, **wkwargs)
