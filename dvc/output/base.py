from dvc.exceptions import DvcException


class OutputError(DvcException):
    def __init__(self, path, msg):
        super(OutputError, self).__init__('Output \'{}\' error: {}'.format(path, msg))


class OutputDoesNotExistError(OutputError):
    def __init__(self, path):
        super(OutputDoesNotExistError, self).__init__(path, 'does not exist')


class OutputIsNotFileOrDirError(OutputError):
    def __init__(self, path):
        super(OutputIsNotFileOrDirError, self).__init__(path, 'not a file or directory')


class OutputAlreadyTrackedError(OutputError):
    def __init__(self, path):
        super(OutputAlreadyTrackedError, self).__init__(path, 'already tracked by scm(e.g. git)')
