from dvc.dependency.base import BaseDependency
from dvc.output.gs import OutputGS


class GSDependency(BaseDependency, OutputGS):
    pass
