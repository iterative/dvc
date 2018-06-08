from dvc.remote.base import RemoteBase


class RemoteSSH(RemoteBase):
    scheme = 'ssh'
    #NOTE: ~/ paths are temporarily forbidden
    REGEX = r'^(?P<user>.*)@(?P<host>.*):(?P<path>/+.*)$'
    pass
