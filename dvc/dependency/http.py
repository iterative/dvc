from dvc.dependency.base import BaseDependency
from dvc.output.base import OutputBase
from dvc.remote.http import HTTPRemote


class HTTPDependency(BaseDependency, OutputBase):
    REMOTE = HTTPRemote
