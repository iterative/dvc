from __future__ import unicode_literals

from dvc.output.gs import OutputGS
from dvc.dependency.base import DependencyBase


class DependencyGS(DependencyBase, OutputGS):
    pass
