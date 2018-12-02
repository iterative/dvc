import traceback


class DvcException(Exception):
    def __init__(self, msg, cause=None):
        self.cause = cause
        self.cause_tb = None
        if cause:
            try:
                self.cause_tb = traceback.format_exc()
            except Exception:  # pragma: no cover
                pass
        super(DvcException, self).__init__(msg)


class UnsupportedRemoteError(DvcException):
    def __init__(self, config):
        msg = "Remote '{}' is not supported.".format(config)
        super(UnsupportedRemoteError, self).__init__(msg)


class OutputDuplicationError(DvcException):
    def __init__(self, output, stages):
        assert isinstance(output, str)
        assert isinstance(stages, list)
        assert all(isinstance(stage, str) for stage in stages)
        msg = "File '{}' is specified as an output in more than one stage:{}"
        s = ""
        for stage in stages:
            s += "\n    " + stage
        super(OutputDuplicationError, self).__init__(msg.format(output, s))


class WorkingDirectoryAsOutputError(DvcException):
    def __init__(self, cwd, fname):
        assert isinstance(cwd, str)
        assert isinstance(fname, str)
        msg = (
            "Current working directory '{cwd}' is specified as an output in"
            " '{fname}'. Use another CWD to prevent any data removal."
            .format(cwd=cwd, fname=fname)
        )
        super(WorkingDirectoryAsOutputError, self).__init__(msg)


class CircularDependencyError(DvcException):
    def __init__(self, dependency):
        assert isinstance(dependency, str)
        msg = "File '{}' is specified as an output and as a dependency."
        super(CircularDependencyError, self).__init__(msg.format(dependency))


class MoveNotDataSourceError(DvcException):
    def __init__(self, path):
        msg = "Move is not permitted for stages that are not data sources. " \
              "You need to either move '{path}' to a new location and edit " \
              "it by hand, or remove '{path}' and create a new one at the " \
              "desired location."
        super(MoveNotDataSourceError, self).__init__(msg.format(path=path))


class NotDvcProjectError(DvcException):
    def __init__(self, root):
        msg = "Not a dvc repository (checked up to mount point {})"
        super(NotDvcProjectError, self).__init__(msg.format(root))


class DvcParserError(DvcException):
    def __init__(self):
        super(DvcException, self).__init__("Parser error")
