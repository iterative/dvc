from typing import Collection, Iterable

from dvc.exceptions import InvalidArgumentError

from .base import ExpRefInfo


class AmbiguousExpRefInfo(InvalidArgumentError):
    def __init__(
        self,
        exp_name: str,
        exp_ref_list: Iterable[ExpRefInfo],
    ):
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
    def __init__(
        self, unresolved_list: Collection[str], *args, git_remote: str = None
    ):
        unresolved_names = ";".join(unresolved_list)
        if not git_remote:
            if len(unresolved_names) > 1:
                super().__init__(
                    f"'{unresolved_names}' are not valid experiment names"
                )
            else:
                super().__init__(
                    f"'{unresolved_names}' is not a valid experiment name"
                )
        else:
            super().__init__(
                f"Experiment '{unresolved_names}' does not exist "
                f"in '{git_remote}'"
            )
