from typing import Dict, Type

from dvc.exceptions import DvcException
from dvc.fs import download as fs_download
from dvc.output import Output


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
        super().__init__(f"DVC file '{path}' cannot be a dependency.")


class Dependency(Output):
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError  # type: Type[DvcException]
    IsNotFileOrDirError = (
        DependencyIsNotFileOrDirError
    )  # type: Type[DvcException]
    IsStageFileError = DependencyIsStageFileError  # type: Type[DvcException]

    def workspace_status(self) -> Dict[str, str]:
        if self.fs.version_aware:
            old_fs_path = self.fs_path
            try:
                self.fs_path = self.fs.path.version_path(self.fs_path, None)
                if self.changed_meta():
                    return {str(self): "update available"}
            finally:
                self.fs_path = old_fs_path
        return super().workspace_status()

    def update(self, rev=None):
        if self.fs.version_aware:
            self.fs_path = self.fs.path.version_path(self.fs_path, rev)
            self.meta = self.get_meta()
            self.def_path = self.fs.path.version_path(
                self.def_path, self.meta.version_id
            )
            self.fs_path = self.fs.path.version_path(
                self.fs_path, self.meta.version_id
            )

    def download(self, to, jobs=None):
        fs_download(self.fs, self.fs_path, to, jobs=jobs)

    def save(self):
        super().save()
        if self.fs.version_aware:
            self.fs_path = self.fs.path.version_path(
                self.fs_path, self.meta.version_id
            )
