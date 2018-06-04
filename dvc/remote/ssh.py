from dvc.remote.base import RemoteBase


class RemoteSSH(RemoteBase):
    #NOTE: ~/ paths are temporarily forbidden
    REGEX = r'^(?P<user>.*)@(?P<host>.*):(?P<path>/+.*)$'
    pass
