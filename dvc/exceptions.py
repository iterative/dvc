"""Exceptions raised by the dvc."""

from __future__ import unicode_literals

from dvc.utils.compat import str, builtin_str

import os
import traceback


class DvcException(Exception):
    """Base class for all dvc exceptions.

    Args:
        msg (unicode): message for this exception.
        cause (Exception): optional cause exception.
    """

    def __init__(self, msg, cause=None):
        # NOTE: unlike python 3, python 2 doesn't have built-in support
        # for chained exceptions, so we are using our own implementation.
        self.cause = cause
        self.cause_tb = None
        if cause:
            try:
                self.cause_tb = traceback.format_exc()
            except AttributeError:  # pragma: no cover
                pass
        super(DvcException, self).__init__(msg)


class OutputDuplicationError(DvcException):
    """Thrown if a file/directory is specified as an output in more than one
    stage.

    Args:
        output (unicode): path to the file/directory.
        stages (list): list of paths to stages.
    """

    def __init__(self, output, stages):
        assert isinstance(output, str) or isinstance(output, builtin_str)
        assert isinstance(stages, list)
        assert all(
            isinstance(stage, str) or isinstance(stage, builtin_str)
            for stage in stages
        )
        msg = (
            "file/directory '{}' is specified as an output in more than one"
            "stage: {}"
        ).format(output, "\n    ".join(stages))
        super(OutputDuplicationError, self).__init__(msg)


class OutputNotFoundError(DvcException):
    """Thrown if a file/directory not found in repository pipelines.

    Args:
        output (unicode): path to the file/directory.
    """

    def __init__(self, output):
        super(OutputNotFoundError, self).__init__(
            "unable to find stage file with output '{path}'".format(
                path=os.path.relpath(output)
            )
        )


class StagePathAsOutputError(DvcException):
    """Thrown if directory that stage is going to be saved in is specified as
    an output of another stage.

    Args:
        cwd (str): path to the directory.
        fname (str): path to the stage file that has cwd specified as an
            output.
    """

    def __init__(self, wdir, fname):
        assert isinstance(wdir, str) or isinstance(wdir, builtin_str)
        assert isinstance(fname, str) or isinstance(fname, builtin_str)
        msg = (
            "current working directory '{cwd}' is specified as an output in"
            " '{fname}'. Use another CWD to prevent any data removal.".format(
                cwd=wdir, fname=fname
            )
        )
        super(StagePathAsOutputError, self).__init__(msg)


class CircularDependencyError(DvcException):
    """Thrown if a file/directory specified both as an output and as a
    dependency.

    Args:
        dependency (str): path to the dependency.
    """

    def __init__(self, dependency):
        assert isinstance(dependency, str) or isinstance(
            dependency, builtin_str
        )

        msg = (
            "file/directory '{}' is specified as an output and as a "
            "dependency."
        )
        super(CircularDependencyError, self).__init__(msg.format(dependency))


class ArgumentDuplicationError(DvcException):
    """Thrown if a file/directory is specified as a dependency/output more
    than once.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        assert isinstance(path, str) or isinstance(path, builtin_str)
        msg = "file '{}' is specified more than once."
        super(ArgumentDuplicationError, self).__init__(msg.format(path))


class MoveNotDataSourceError(DvcException):
    """Thrown if attempted to move a file/directory that is not an output
    in a data source stage.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        msg = (
            "move is not permitted for stages that are not data sources. "
            "You need to either move '{path}' to a new location and edit "
            "it by hand, or remove '{path}' and create a new one at the "
            "desired location."
        )
        super(MoveNotDataSourceError, self).__init__(msg.format(path=path))


class NotDvcRepoError(DvcException):
    """Thrown if a directory is not a dvc repo.

    Args:
        root (str): path to the directory.
    """

    def __init__(self, root):
        msg = (
            "you are not inside of a dvc repository "
            "(checked up to mount point '{}')"
        )
        super(NotDvcRepoError, self).__init__(msg.format(root))


class DvcParserError(DvcException):
    """Base class for CLI parser errors."""

    def __init__(self):
        super(DvcParserError, self).__init__("parser error")


class CyclicGraphError(DvcException):
    def __init__(self, stages):
        assert isinstance(stages, list)
        stages = "\n".join("\t- {}".format(stage) for stage in stages)
        msg = (
            "you've introduced a cycle in your pipeline that involves"
            " the following stages:"
            "\n"
            "{stages}".format(stages=stages)
        )
        super(CyclicGraphError, self).__init__(msg)


class ConfirmRemoveError(DvcException):
    def __init__(self, path):
        super(ConfirmRemoveError, self).__init__(
            "unable to remove '{}' without a confirmation from the user. Use "
            "'-f' to force.".format(path)
        )


class InitError(DvcException):
    def __init__(self, msg):
        super(InitError, self).__init__(msg)


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name, ex):
        self.path = dvc_file_name
        msg = "failed to reproduce '{}'".format(dvc_file_name)
        super(ReproductionError, self).__init__(msg, cause=ex)


class BadMetricError(DvcException):
    def __init__(self, path):
        super(BadMetricError, self).__init__(
            "'{}' does not exist, not a metric or is malformed".format(
                os.path.relpath(path)
            )
        )


class NoMetricsError(DvcException):
    def __init__(self):
        super(NoMetricsError, self).__init__(
            "no metric files in this repository. "
            "Use 'dvc metrics add' to add a metric file to track."
        )


class StageFileCorruptedError(DvcException):
    def __init__(self, path, cause=None):
        path = os.path.relpath(path)
        super(StageFileCorruptedError, self).__init__(
            "unable to read stage file: {} "
            "YAML file structure is corrupted".format(path),
            cause=cause,
        )


class RecursiveAddingWhileUsingFilename(DvcException):
    def __init__(self):
        super(RecursiveAddingWhileUsingFilename, self).__init__(
            "using fname with recursive is not allowed."
        )


class OverlappingOutputPathsError(DvcException):
    def __init__(self, out_1, out_2):
        super(OverlappingOutputPathsError, self).__init__(
            "Paths for outs:\n'{}'('{}')\n'{}'('{}')\noverlap. To avoid "
            "unpredictable behaviour, rerun command with non overlapping outs "
            "paths.".format(
                str(out_1),
                out_1.stage.relpath,
                str(out_2),
                out_2.stage.relpath,
            )
        )


class TargetNotDirectoryError(DvcException):
    def __init__(self, path):
        super(TargetNotDirectoryError, self).__init__(
            "Target: {} is not a directory".format(path)
        )


class CheckoutErrorSuggestGit(DvcException):
    def __init__(self, target, cause):
        super(CheckoutErrorSuggestGit, self).__init__(
            "Did you mean 'git checkout {}'?".format(target), cause=cause
        )


class ETagMismatchError(DvcException):
    def __init__(self, etag, cached_etag):
        super(ETagMismatchError, self).__init__(
            "ETag mismatch detected when copying file to cache! "
            "(expected: '{}', actual: '{}')".format(etag, cached_etag)
        )
