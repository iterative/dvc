from __future__ import unicode_literals

from dvc.output.base import OutputBase
from dvc.remote.http import RemoteHTTP
from dvc.dependency.base import DependencyBase


class DependencyHTTP(DependencyBase, OutputBase):
    REMOTE = RemoteHTTP
