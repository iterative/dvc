class NeatLynxException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class GitCmdError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, msg)


class ConfigError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Config file error: {}'.format(msg))