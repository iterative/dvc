from dvc.dependency.base import BaseDependency
from dvc.output.hdfs import HDFSOutput


class HDFSDependency(BaseDependency, HDFSOutput):
    pass
