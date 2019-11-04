from __future__ import unicode_literals

from dvc.dependency.base import DependencyBase
from dvc.output.ssh import OutputSSH


class DependencySSH(DependencyBase, OutputSSH):
    pass
