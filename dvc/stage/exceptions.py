from dvc.exceptions import DvcException


class StageCmdFailedError(DvcException):
    def __init__(self, stage, status=None):
        msg = "failed to run: {}".format(stage.cmd)
        if status is not None:
            msg += ", exited with {}".format(status)
        super().__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self, fname, e):
        msg = "DVC-file '{}' format error: {}".format(fname, str(e))
        super().__init__(msg)


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import DVC_FILE_SUFFIX, is_dvc_file

        msg = "'{}' does not exist.".format(fname)

        sname = fname + DVC_FILE_SUFFIX
        if is_dvc_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super().__init__(msg)


class StageFileAlreadyExistsError(DvcException):
    def __init__(self, relpath):
        msg = "not overwriting '{}'".format(relpath)
        super().__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import DVC_FILE_SUFFIX, is_dvc_file

        msg = "'{}' is not a DVC-file".format(fname)

        sname = fname + DVC_FILE_SUFFIX
        if is_dvc_file(sname):
            msg += " Do you mean '{}'?".format(sname)

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
        super().__init__(
            "Stage '{}' not found inside '{}' file".format(name, file.relpath)
        )


class StageNameUnspecified(DvcException):
    def __init__(self, file):
        super().__init__(
            "Stage name not provided."
            "Please specify the name as: `{0}:stage_name`".format(file.relpath)
        )


class DuplicateStageName(DvcException):
    def __init__(self, name, file):
        super().__init__(
            "Stage '{name}' already exists in '{relpath}'.".format(
                name=name, relpath=file.relpath
            )
        )


class InvalidStageName(DvcException):
    def __init__(self,):
        super().__init__(
            "Stage name cannot contain invalid characters: "
            "'\\', '/', '@' and ':'."
        )
