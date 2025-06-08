from collections.abc import Collection

from dvc.exceptions import DvcException


class CannotKillTasksError(DvcException):
    def __init__(self, revs: Collection[str]):
        rev_str = ",".join(revs)
        super().__init__(
            f"Task '{rev_str}' is initializing, please wait a few seconds "
            "until the experiments start running to retry the kill operation."
        )
