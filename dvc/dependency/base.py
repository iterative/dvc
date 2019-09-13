from __future__ import unicode_literals

from dvc.exceptions import DvcException


class DependencyDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = "dependency '{}' does not exist".format(path)
        super(DependencyDoesNotExistError, self).__init__(msg)


class DependencyIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = "dependency '{}' is not a file or directory".format(path)
        super(DependencyIsNotFileOrDirError, self).__init__(msg)


class DependencyIsStageFileError(DvcException):
    def __init__(self, path):
        super(DependencyIsStageFileError, self).__init__(
            "Stage file '{}' cannot be a dependency.".format(path)
        )


class DependencyBase(object):
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError
    IsStageFileError = DependencyIsStageFileError

    def update(self):
        pass
