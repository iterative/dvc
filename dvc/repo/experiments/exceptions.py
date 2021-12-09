from dvc.exceptions import InvalidArgumentError
from dvc.types import List


class UnresolvedExpNamesError(InvalidArgumentError):
    def __init__(
        self, unresolved_list: List[str], *args, git_remote: str = None
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
