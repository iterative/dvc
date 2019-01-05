from dvc.output.ssh import OutputSSH
from dvc.dependency.base import DependencyBase


class DependencySSH(DependencyBase, OutputSSH):
    pass
