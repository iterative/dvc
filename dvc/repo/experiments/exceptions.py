from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING, Optional

from dvc.exceptions import DvcException, InvalidArgumentError

if TYPE_CHECKING:
    from .refs import ExpRefInfo


class BaselineMismatchError(DvcException):
    def __init__(self, rev, expected):
        if hasattr(rev, "hexsha"):
            rev = rev.hexsha
        rev_str = f"{rev[:7]}" if rev is not None else "invalid commit"
        super().__init__(
            f"Experiment derived from '{rev_str}', expected '{expected[:7]}'."
        )
        self.rev = rev
        self.expected_rev = expected


class ExperimentExistsError(DvcException):
    def __init__(self, name: str, command: str = "run"):
        msg = (
            "Experiment conflicts with existing experiment "
            f"'{name}'. To overwrite the existing experiment run:\n\n"
            f"\tdvc exp {command} -f ...\n\n"
        )
        super().__init__(msg)
        self.name = name


class InvalidExpRefError(DvcException):
    def __init__(self, ref):
        super().__init__(f"'{ref}' is not a valid experiment refname.")
        self.ref = ref


class InvalidExpRevError(InvalidArgumentError):
    def __init__(self, rev):
        super().__init__(f"'{rev}' does not appear to be an experiment commit.")


class MultipleBranchError(DvcException):
    def __init__(self, rev, ref_infos):
        super().__init__(
            f"Ambiguous commit '{rev[:7]}' belongs to multiple experiment branches."
        )
        self.rev = rev
        self.ref_infos = ref_infos


class AmbiguousExpRefInfo(InvalidArgumentError):  # noqa: N818
    def __init__(self, exp_name: str, exp_ref_list: Iterable["ExpRefInfo"]):
        msg = [
            (
                f"Ambiguous name '{exp_name}' refers to multiple experiments."
                " Use one of the following full refnames instead:"
            ),
            "",
        ]
        msg.extend([f"\t{info}" for info in exp_ref_list])
        super().__init__("\n".join(msg))


class UnresolvedExpNamesError(InvalidArgumentError):
    NAME = "experiment name"

    def __init__(
        self,
        unresolved_list: Collection[str],
        *args,
        git_remote: Optional[str] = None,
    ):
        unresolved_names = "; ".join(unresolved_list)
        if not git_remote:
            if len(unresolved_list) > 1:
                super().__init__(f"'{unresolved_names}' are not valid {self.NAME}s")
            else:
                super().__init__(f"'{unresolved_names}' is not a valid {self.NAME}")
        else:
            super().__init__(
                f"Experiment '{unresolved_names}' does not exist in '{git_remote}'"
            )


class UnresolvedQueueExpNamesError(UnresolvedExpNamesError):
    NAME = "queued experiment name"


class UnresolvedRunningExpNamesError(UnresolvedExpNamesError):
    NAME = "running experiment name"


class ExpQueueEmptyError(DvcException):
    pass


class ExpNotStartedError(DvcException):
    def __init__(self, name: str):
        super().__init__(
            f"Queued experiment '{name}' exists but has not started running yet"
        )
