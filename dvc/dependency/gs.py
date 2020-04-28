from dvc.dependency.base import BaseDependency
from dvc.output.gs import GSOutput


class GSDependency(BaseDependency, GSOutput):
    pass
