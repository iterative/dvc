from dvc.dependency.base import DependencyBase
from dvc.output.base import OutputBase
from dvc.remote.http import RemoteHTTP


class DependencyHTTP(DependencyBase, OutputBase):
    REMOTE = RemoteHTTP
