from dvc.dependency.base import DependencyBase
from dvc.output.base import OutputBase
from dvc.remote.http import HTTPRemote


class DependencyHTTP(DependencyBase, OutputBase):
    REMOTE = HTTPRemote
