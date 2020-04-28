from dvc.dependency.base import BaseDependency
from dvc.output.local import OutputLOCAL


class LocalDependency(BaseDependency, OutputLOCAL):
    pass
