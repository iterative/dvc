from dvc.dependency.base import DependencyBase
from dvc.output.local import OutputLOCAL


class DependencyLOCAL(DependencyBase, OutputLOCAL):
    pass
