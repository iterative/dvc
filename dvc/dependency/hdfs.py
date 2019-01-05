from dvc.output.hdfs import OutputHDFS
from dvc.dependency.base import DependencyBase


class DependencyHDFS(DependencyBase, OutputHDFS):
    pass
