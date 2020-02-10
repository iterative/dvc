"""Exceptions raised by the dvc."""

from dvc.utils import relpath, format_link


class DvcException(Exception):
    """Base class for all dvc exceptions."""


class OutputDuplicationError(DvcException):
    """Thrown if a file/directory is specified as an output in more than one
    stage.

    Args:
        output (unicode): path to the file/directory.
        stages (list): list of paths to stages.
    """

    def __init__(self, output, stages):
        assert isinstance(output, str)
        assert all(hasattr(stage, "relpath") for stage in stages)
        msg = (
            "file/directory '{}' is specified as an output in more than one "
            "stage: {}\n"
            "This is not allowed. Consider using a different output name."
        ).format(output, "\n    ".join(s.relpath for s in stages))
        super().__init__(msg)


class OutputNotFoundError(DvcException):
    """Thrown if a file/directory not found in repository pipelines.

    Args:
        output (unicode): path to the file/directory.
    """

    def __init__(self, output, repo=None):
        self.output = output
        self.repo = repo
        super().__init__(
            "Unable to find DVC-file with output '{path}'".format(
                path=relpath(self.output)
            )
        )


class StagePathAsOutputError(DvcException):
    """Thrown if directory that stage is going to be saved in is specified as
    an output of another stage.

    Args:
        stage (Stage): a stage that is in some other stages output
        output (str): an output covering the stage above
    """

    def __init__(self, stage, output):
        assert isinstance(output, str)
        super().__init__(
            "'{stage}' is within an output '{output}' of another stage".format(
                stage=stage.relpath, output=output
            )
        )


class CircularDependencyError(DvcException):
    """Thrown if a file/directory specified both as an output and as a
    dependency.

    Args:
        dependency (str): path to the dependency.
    """

    def __init__(self, dependency):
        assert isinstance(dependency, str)

        msg = (
            "file/directory '{}' is specified as an output and as a "
            "dependency."
        )
        super().__init__(msg.format(dependency))


class ArgumentDuplicationError(DvcException):
    """Thrown if a file/directory is specified as a dependency/output more
    than once.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        assert isinstance(path, str)
        super().__init__("file '{}' is specified more than once.".format(path))


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
        super().__init__(msg.format(path=path))


class NotDvcRepoError(DvcException):
    """Thrown if a directory is not a DVC repo"""


class DvcParserError(DvcException):
    """Base class for CLI parser errors."""

    def __init__(self):
        super().__init__("parser error")


class CyclicGraphError(DvcException):
    def __init__(self, stages):
        assert isinstance(stages, list)
        stages = "\n".join("\t- {}".format(stage.relpath) for stage in stages)
        msg = (
            "you've introduced a cycle in your pipeline that involves "
            "the following stages:"
            "\n"
            "{stages}".format(stages=stages)
        )
        super().__init__(msg)


class ConfirmRemoveError(DvcException):
    def __init__(self, path):
        super().__init__(
            "unable to remove '{}' without a confirmation from the user. Use "
            "`-f` to force.".format(path)
        )


class InitError(DvcException):
    pass


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name):
        self.path = dvc_file_name
        super().__init__("failed to reproduce '{}'".format(dvc_file_name))


class BadMetricError(DvcException):
    def __init__(self, paths):
        super().__init__(
            "the following metrics do not exist, "
            "are not metric files or are malformed: {paths}".format(
                paths=", ".join("'{}'".format(path) for path in paths)
            )
        )


class NoMetricsError(DvcException):
    def __init__(self):
        super().__init__(
            "no metric files in this repository. "
            "Use `dvc metrics add` to add a metric file to track."
        )


class StageFileCorruptedError(DvcException):
    def __init__(self, path):
        path = relpath(path)
        super().__init__(
            "unable to read DVC-file: {} "
            "YAML file structure is corrupted".format(path)
        )


class RecursiveAddingWhileUsingFilename(DvcException):
    def __init__(self):
        super().__init__(
            "cannot use `fname` with multiple targets or `-R|--recursive`"
        )


class OverlappingOutputPathsError(DvcException):
    def __init__(self, out_1, out_2):
        super().__init__(
            "Paths for outs:\n'{}'('{}')\n'{}'('{}')\noverlap. To avoid "
            "unpredictable behaviour, rerun command with non overlapping outs "
            "paths.".format(
                str(out_1),
                out_1.stage.relpath,
                str(out_2),
                out_2.stage.relpath,
            )
        )


class CheckoutErrorSuggestGit(DvcException):
    def __init__(self, target):
        super().__init__("Did you mean `git checkout {}`?".format(target))


class ETagMismatchError(DvcException):
    def __init__(self, etag, cached_etag):
        super().__init__(
            "ETag mismatch detected when copying file to cache! "
            "(expected: '{}', actual: '{}')".format(etag, cached_etag)
        )


class FileMissingError(DvcException):
    def __init__(self, path):
        self.path = path
        super().__init__(
            "Can't find '{}' neither locally nor on remote".format(path)
        )


class DvcIgnoreInCollectedDirError(DvcException):
    def __init__(self, ignore_dirname):
        super().__init__(
            ".dvcignore file should not be in collected dir path: "
            "'{}'".format(ignore_dirname)
        )


class GitHookAlreadyExistsError(DvcException):
    def __init__(self, hook_name):
        super().__init__(
            "Hook '{}' already exists. Please refer to {} for more "
            "info.".format(
                hook_name, format_link("https://man.dvc.org/install")
            )
        )


class DownloadError(DvcException):
    def __init__(self, amount):
        self.amount = amount

        super().__init__(
            "{amount} files failed to download".format(amount=amount)
        )


class UploadError(DvcException):
    def __init__(self, amount):
        self.amount = amount

        super().__init__(
            "{amount} files failed to upload".format(amount=amount)
        )


class CheckoutError(DvcException):
    def __init__(self, target_infos):
        targets = [str(t) for t in target_infos]
        m = (
            "Checkout failed for following targets:\n {}\nDid you "
            "forget to fetch?".format("\n".join(targets))
        )
        super().__init__(m)


class CollectCacheError(DvcException):
    pass


class NoRemoteInExternalRepoError(DvcException):
    def __init__(self, url):
        super().__init__(
            "No DVC remote is specified in target repository '{}'.".format(url)
        )


class NoOutputInExternalRepoError(DvcException):
    def __init__(self, path, external_repo_path, external_repo_url):
        super().__init__(
            "Output '{}' not found in target repository '{}'".format(
                relpath(path, external_repo_path), external_repo_url
            )
        )


class HTTPError(DvcException):
    def __init__(self, code, reason):
        super().__init__("'{} {}'".format(code, reason))


class PathMissingError(DvcException):
    def __init__(self, path, repo):
        msg = (
            "The path '{}' does not exist in the target repository '{}'"
            " neither as an output nor a git-handled file."
        )
        super().__init__(msg.format(path, repo))


class RemoteCacheRequiredError(DvcException):
    def __init__(self, path_info):
        super().__init__(
            (
                "Current operation was unsuccessful because '{}' requires "
                "existing cache on '{}' remote. See {} for information on how "
                "to set up remote cache."
            ).format(
                path_info,
                path_info.scheme,
                format_link("https://man.dvc.org/config#cache"),
            )
        )
