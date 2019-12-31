from dvc.dependency.base import DependencyBase
from dvc.output.gs import OutputGS


class DependencyGS(DependencyBase, OutputGS):
    pass
