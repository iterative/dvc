"""Exceptions raised by the dvc."""
import errno
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from dvc.utils import format_link

if TYPE_CHECKING:
    from dvc.stage import Stage


class DvcException(Exception):
    """Base class for all dvc exceptions."""

    def __init__(self, msg, *args):
        assert msg
        self.msg = msg
        super().__init__(msg, *args)


class InvalidArgumentError(ValueError, DvcException):
    """Thrown if arguments are invalid."""

    def __init__(self, msg, *args):
        self.msg = msg
        super().__init__(msg, *args)


class OutputDuplicationError(DvcException):
    """Thrown if a file/directory is specified as an output in more than one
    stage.

    Args:
        output (unicode): path to the file/directory.
        stages (list): list of paths to stages.
    """

    def __init__(self, output: str, stages: Set["Stage"]):
        from funcy import first

        assert isinstance(output, str)
        assert all(hasattr(stage, "relpath") for stage in stages)
        if len(stages) == 1:
            stage = first(stages)
            msg = (
                f"output '{output}' is already specified in {stage}."
                f"\nUse `dvc remove {stage.addressing}` to stop tracking the "
                "overlapping output."
            )
        else:
            stage_names = "\n".join(["\t- " + s.addressing for s in stages])
            msg = (
                f"output '{output}' is specified in:\n{stage_names}"
                "\nUse `dvc remove` with any of the above targets to stop tracking the "
                "overlapping output."
            )
        super().__init__(msg)
        self.stages = stages
        self.output = output


class OutputNotFoundError(DvcException):
    """Thrown if a file/directory is not found as an output in any pipeline.

    Args:
        output (unicode): path to the file/directory.
    """

    def __init__(self, output, repo=None):
        from dvc.utils import relpath

        self.output = output
        self.repo = repo
        super().__init__(
            f"Unable to find DVC file with output {relpath(self.output)!r}"
        )


class StageNotFoundError(DvcException):
    pass


class StagePathAsOutputError(DvcException):
    """Thrown if directory that stage is going to be saved in is specified as
    an output of another stage.

    Args:
        stage (Stage): a stage that is in some other stages output
        output (str): an output covering the stage above
    """

    def __init__(self, stage, output):
        assert isinstance(output, str)
        super().__init__(f"{stage} is within an output {output!r} of another stage")


class CircularDependencyError(DvcException):
    """Thrown if a file/directory specified both as an output and as a
    dependency.

    Args:
        dependency (str): path to the dependency.
    """

    def __init__(self, dependency):
        assert isinstance(dependency, str)

        msg = "'{}' is specified as an output and as a dependency."
        super().__init__(msg.format(dependency))


class ArgumentDuplicationError(DvcException):
    """Thrown if a file/directory is specified as a dependency/output more
    than once.

    Args:
        path (str): path to the file/directory.
    """

    def __init__(self, path):
        assert isinstance(path, str)
        super().__init__(f"file '{path}' is specified more than once.")


class MoveNotDataSourceError(DvcException):
    """Thrown when trying to move a file/directory that is not an output
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


class CyclicGraphError(DvcException):
    def __init__(self, stages):
        assert isinstance(stages, list)
        stage_part = "stage" if len(stages) == 1 else "stages"
        msg = (
            "Same item(s) are defined as both a dependency and an output "
            "in {stage_part}: {stage}."
        )
        super().__init__(
            msg.format(
                stage_part=stage_part,
                stage=", ".join(s.addressing for s in stages),
            )
        )


class ConfirmRemoveError(DvcException):
    def __init__(self, path):
        super().__init__(
            f"unable to remove {path!r} without a confirmation. Use `-f` to force."
        )


class InitError(DvcException):
    pass


class ReproductionError(DvcException):
    pass


class BadMetricError(DvcException):
    def __init__(self, paths):
        super().__init__(
            "the following metrics do not exist, "
            "are not metrics files or are malformed: {paths}".format(
                paths=", ".join(f"'{path}'" for path in paths)
            )
        )


class OverlappingOutputPathsError(DvcException):
    def __init__(self, parent, overlapping_out, message):
        self.parent = parent
        self.overlapping_out = overlapping_out
        super().__init__(message)


class CheckoutErrorSuggestGit(DvcException):
    def __init__(self, target):
        super().__init__(f"Did you mean `git checkout {target}`?")


class ETagMismatchError(DvcException):
    def __init__(self, etag, cached_etag):
        super().__init__(
            "ETag mismatch detected when copying file to cache! "
            f"(expected: '{etag}', actual: '{cached_etag}')"
        )


class FileExistsLocallyError(FileExistsError, DvcException):
    def __init__(self, path, hint=None):
        import os.path

        self.path = path
        hint = "" if hint is None else f". {hint}"
        path_typ = "directory" if os.path.isdir(path) else "file"
        msg = f"The {path_typ} '{path}' already exists locally{hint}"
        super().__init__(msg)
        self.errno = errno.EEXIST


class FileMissingError(DvcException):
    def __init__(self, path, hint=None):
        self.path = path
        hint = "" if hint is None else f". {hint}"
        super().__init__(f"Can't find '{path}' neither locally nor on remote{hint}")


class FileTransferError(DvcException):
    _METHOD = "transfer"

    def __init__(self, amount):
        self.amount = amount

        super().__init__(f"{amount} files failed to {self._METHOD}")


class DownloadError(FileTransferError):
    _METHOD = "download"


class UploadError(FileTransferError):
    _METHOD = "upload"


class CheckoutError(DvcException):
    def __init__(self, target_infos: List[str], stats: Dict[str, List[str]]):
        from dvc.utils import error_link

        self.target_infos = target_infos
        self.stats = stats
        targets = [str(t) for t in target_infos]
        m = (
            "Checkout failed for following targets:\n{}\nIs your "
            "cache up to date?\n{}".format(
                "\n".join(targets), error_link("missing-files")
            )
        )
        super().__init__(m)


class CollectCacheError(DvcException):
    pass


class NoRemoteInExternalRepoError(DvcException):
    def __init__(self, url):
        super().__init__(f"No DVC remote is specified in target repository '{url}'.")


class NoOutputInExternalRepoError(DvcException):
    def __init__(self, path, external_repo_path, external_repo_url):
        from dvc.utils import relpath

        super().__init__(
            "Output '{}' not found in target repository '{}'".format(
                relpath(path, external_repo_path), external_repo_url
            )
        )


class HTTPError(DvcException):
    def __init__(self, code, reason):
        super().__init__(f"'{code} {reason}'")


class PathMissingError(DvcException):
    default_msg = (
        "The path '{}' does not exist in the target repository '{}'"
        " neither as a DVC output nor as a Git-tracked file."
    )
    default_msg_dvc_only = (
        "The path '{}' does not exist in the target repository '{}' as an DVC output."
    )

    def __init__(self, path, repo, dvc_only=False):
        msg = self.default_msg if not dvc_only else self.default_msg_dvc_only
        super().__init__(msg.format(path, repo))
        self.dvc_only = dvc_only


class URLMissingError(DvcException):
    def __init__(self, url):
        super().__init__(f"The path '{url}' does not exist")


class IsADirectoryError(DvcException):  # noqa: A001
    """Raised when a file operation is requested on a directory."""


class NoOutputOrStageError(DvcException):
    """
    Raised when the target is neither an output nor a stage name in dvc.yaml
    """

    def __init__(self, target, file):
        super().__init__(
            f"'{target}' does not exist as an output or a stage name in '{file}'"
        )


class MergeError(DvcException):
    pass


class CacheLinkError(DvcException):
    SUPPORT_LINK = "See {} for more information.".format(
        format_link("https://dvc.org/doc/user-guide/troubleshooting#cache-types")
    )

    def __init__(self, fs_paths):
        msg = "No possible cache link types for '{}'. {}".format(
            ", ".join(fs_paths), self.SUPPORT_LINK
        )
        super().__init__(msg)
        self.fs_paths = fs_paths


class PrettyDvcException(DvcException):
    def __pretty_exc__(self, **kwargs):
        """Print prettier exception message."""


class ArtifactNotFoundError(DvcException):
    """Thrown if an artifact is not found in the DVC repo.

    Args:
        name (str): artifact name.
    """

    def __init__(
        self,
        name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None,
    ):
        self.name = name
        self.version = version
        self.stage = stage

        desc = f" @ {stage or version}" if (stage or version) else ""
        super().__init__(f"Unable to find artifact '{name}{desc}'")


class RevCollectionError(DvcException):
    """Thrown if a revision failed to be collected.

    Args:
        rev (str): revision that failed (or "workspace").
    """

    def __init__(self, rev):
        self.rev = rev
        super().__init__(f"Failed to collect '{rev}'")
