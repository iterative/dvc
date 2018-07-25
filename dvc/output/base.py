from dvc.exceptions import DvcException


class OutputError(DvcException):
    def __init__(self, path, msg):
        msg = 'Output \'{}\' error: {}'.format(path, msg)
        super(OutputError, self).__init__(msg)


class OutputDoesNotExistError(OutputError):
    def __init__(self, path):
        super(OutputDoesNotExistError, self).__init__(path, 'does not exist')


class OutputIsNotFileOrDirError(OutputError):
    def __init__(self, path):
        msg = 'not a file or directory'
        super(OutputIsNotFileOrDirError, self).__init__(path, msg)


class OutputAlreadyTrackedError(OutputError):
    def __init__(self, path):
        msg = 'already tracked by scm(e.g. git)'
        super(OutputAlreadyTrackedError, self).__init__(path, msg)
