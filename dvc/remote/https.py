from __future__ import unicode_literals

from dvc.scheme import Schemes

from .http import RemoteHTTP


class RemoteHTTPS(RemoteHTTP):
    scheme = Schemes.HTTPS
    REGEX = r"^https://.*$"
