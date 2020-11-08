from dvc.output.base import BaseOutput

from ..tree.gdrive import GDriveTree


class GDriveOutput(BaseOutput):
    TREE_CLS = GDriveTree

    def get_file_name(self):
        return self.tree.get_file_name(self.path_info)
