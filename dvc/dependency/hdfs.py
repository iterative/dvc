from dvc.dependency.base import BaseDependency
from dvc.output.hdfs import OutputHDFS


class HDFSDependency(BaseDependency, OutputHDFS):
    pass
