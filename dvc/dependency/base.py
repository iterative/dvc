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


class DependencyBase(object):
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError
