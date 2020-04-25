from dvc.exceptions import DvcException


class StageCmdFailedError(DvcException):
    def __init__(self, stage):
        msg = "stage '{}' cmd '{}' failed".format(stage.relpath, stage.cmd)
        super().__init__(msg)


class StageFileFormatError(DvcException):
    def __init__(self, fname, e):
        msg = "DVC-file '{}' format error: {}".format(fname, str(e))
        super().__init__(msg)


class StageFileDoesNotExistError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import DVC_FILE_SUFFIX, Dvcfile

        msg = "'{}' does not exist.".format(fname)

        sname = fname + DVC_FILE_SUFFIX
        if Dvcfile.is_stage_file(sname):
            msg += " Do you mean '{}'?".format(sname)

        super().__init__(msg)


class StageFileAlreadyExistsError(DvcException):
    def __init__(self, relpath):
        msg = "not overwriting '{}'".format(relpath)
        super().__init__(msg)


class StageFileIsNotDvcFileError(DvcException):
    def __init__(self, fname):
        from dvc.dvcfile import Dvcfile, DVC_FILE_SUFFIX

        msg = "'{}' is not a DVC-file".format(fname)

        sname = fname + DVC_FILE_SUFFIX
        if Dvcfile.is_stage_file(sname):
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


class MissingDep(DvcException):
    def __init__(self, deps):
        assert len(deps) > 0

        dep = "dependencies" if len(deps) > 1 else "dependency"
        msg = "missing '{}': {}".format(dep, ", ".join(map(str, deps)))
        super().__init__(msg)


class MissingDataSource(DvcException):
    def __init__(self, missing_files):
        assert len(missing_files) > 0

        source = "source"
        if len(missing_files) > 1:
            source += "s"

        msg = "missing data '{}': {}".format(source, ", ".join(missing_files))
        super().__init__(msg)
