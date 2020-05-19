from dvc.exceptions import DvcException


class DependencyDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' does not exist"
        super().__init__(msg)


class DependencyIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' is not a file or directory"
        super().__init__(msg)


class DependencyIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"Stage file '{path}' cannot be a dependency.")


class BaseDependency:
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError
    IsNotFileOrDirError = DependencyIsNotFileOrDirError
    IsStageFileError = DependencyIsStageFileError

    def update(self, rev=None):
        pass
