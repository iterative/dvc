from dvc.dependency.base import BaseDependency
from dvc.output.ssh import OutputSSH


class SSHDependency(BaseDependency, OutputSSH):
    pass
