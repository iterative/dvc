from dvc.exceptions import DvcException


class StageCmdFailedError(DvcException):
    def __init__(self, cmd, status=None):
        msg = f"failed to run: {cmd}"
        if status is not None:
            msg += f", exited with {status}"
        super().__init__(msg)


class StageFileDoesNotExistError(DvcException):
    DVC_IGNORED = "is dvc-ignored"
    DOES_NOT_EXIST = "does not exist"

    def __init__(self, fname, dvc_ignored=False):
        self.file = fname
        message = self.DVC_IGNORED if dvc_ignored else self.DOES_NOT_EXIST
        super().__init__(f"'{self.file}' {message}")


class StageFileAlreadyExistsError(DvcException):
    pass


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import DVC_FILE_SUFFIX, is_dvc_file

        msg = f"'{fname}' is not a .dvc file"

        sname = fname + DVC_FILE_SUFFIX
        if is_dvc_file(sname):
            msg += f". Do you mean '{sname}'?"

        super().__init__(msg)


class StageFileBadNameError(DvcException):
    pass


class StagePathOutsideError(DvcException):
    pass


class StagePathNotFoundError(DvcException):
    pass


class StagePathNotDirectoryError(DvcException):
    pass


class StageCommitError(DvcException):
    pass


class StageExternalOutputsError(DvcException):
    pass


class StageUpdateError(DvcException):
    def __init__(self, path):
        super().__init__(f"update is not supported for '{path}' that is not an import.")


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = "source"
        if len(missing_files) > 1:
            source += "s"

        msg = "missing data '{}': {}".format(source, ", ".join(missing_files))
        super().__init__(msg)


class DataSourceChanged(DvcException):
    def __init__(self, path: str):
        super().__init__(f"data source changed: {path}")


class StageNotFound(DvcException, KeyError):
    def __init__(self, file, name):
        self.file = file.relpath
        self.name = name
        super().__init__(f"Stage '{self.name}' not found inside '{self.file}' file")

    def __str__(self):
        # `KeyError` quotes the message
        # see: https://bugs.python.org/issue2651
        return self.msg


class StageNameUnspecified(DvcException):
    def __init__(self, file):
        super().__init__(
            "Stage name not provided."
            f"Please specify the name as: `{file.relpath}:stage_name`"
        )


class DuplicateStageName(DvcException):
    pass


class InvalidStageName(DvcException):
    def __init__(self):
        super().__init__("Stage name cannot contain punctuation characters.")
