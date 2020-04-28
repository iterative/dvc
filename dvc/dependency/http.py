from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput
from dvc.remote.http import HTTPRemote


class HTTPDependency(BaseDependency, BaseOutput):
    REMOTE = HTTPRemote
