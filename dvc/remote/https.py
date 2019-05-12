from __future__ import unicode_literals

from dvc.path import Schemes

from .http import RemoteHTTP


class RemoteHTTPS(RemoteHTTP):
    scheme = Schemes.HTTPS
    REGEX = r"^https://.*$"
