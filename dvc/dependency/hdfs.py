from dvc.dependency.base import DependencyBase
from dvc.output.hdfs import OutputHDFS


class DependencyHDFS(DependencyBase, OutputHDFS):
    pass
