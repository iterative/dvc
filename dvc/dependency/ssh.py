from dvc.dependency.base import BaseDependency
from dvc.output.ssh import SSHOutput


class SSHDependency(BaseDependency, SSHOutput):
    pass
