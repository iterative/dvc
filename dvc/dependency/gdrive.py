from dvc.dependency.base import BaseDependency
from dvc.output.gdrive import GDriveOutput
from dvc.tree import GDriveTree
from dvc.tree.gdrive import GDriveURLInfo


class GDriveDependency(BaseDependency, GDriveOutput):
    def __init__(self, stage, path, info=None, **wkwargs):
        repo = stage.repo if stage else None
        base_path = GDriveURLInfo(path).replace(path="").url
        tree = GDriveTree(repo, {"url": base_path})
        GDriveOutput.__init__(self, stage, path, info, tree=tree, **wkwargs)
