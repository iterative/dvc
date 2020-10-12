from dvc.exceptions import DvcException


class StageCmdFailedError(DvcException):
    def __init__(self, cmd, status=None):
        msg = f"failed to run: {cmd}"
        if status is not None:
            msg += f", exited with {status}"
        super().__init__(msg)


class StageFileFormatError(DvcException):
    pass


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        self.file = fname
        super().__init__(f"'{self.file}' does not exist.")


class StageFileAlreadyExistsError(DvcException):
    pass


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import DVC_FILE_SUFFIX, is_dvc_file

        msg = f"'{fname}' is not a DVC-file"

        sname = fname + DVC_FILE_SUFFIX
        if is_dvc_file(sname):
            msg += f" Do you mean '{sname}'?"

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
        super().__init__(
            "update is not supported for '{}' that is not an "
            "import.".format(path)
        )


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = "source"
        if len(missing_files) > 1:
            source += "s"

        msg = "missing data '{}': {}".format(source, ", ".join(missing_files))
        super().__init__(msg)


class StageNotFound(KeyError, DvcException):
    def __init__(self, file, name):
        self.file = file.relpath
        self.name = name
        super().__init__(
            f"Stage '{self.name}' not found inside '{self.file}' file"
        )


class StageNameUnspecified(DvcException):
    def __init__(self, file):
        super().__init__(
            "Stage name not provided."
            "Please specify the name as: `{}:stage_name`".format(file.relpath)
        )


class DuplicateStageName(DvcException):
    pass


class InvalidStageName(DvcException):
    def __init__(self):
        super().__init__("Stage name cannot contain punctuation characters.")
