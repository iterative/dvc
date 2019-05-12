from __future__ import unicode_literals

from dvc.remote.https import RemoteHTTPS
from .http import DependencyHTTP


class DependencyHTTPS(DependencyHTTP):
    REMOTE = RemoteHTTPS
