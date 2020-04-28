from dvc.dependency.base import BaseDependency
from dvc.output.local import LocalOutput


class LocalDependency(BaseDependency, LocalOutput):
    pass
