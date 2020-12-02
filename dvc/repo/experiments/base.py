from typing import Optional

from dvc.exceptions import DvcException

# Experiment refs are stored according baseline git SHA:
#   refs/exps/01/234abcd.../<exp_name>
EXPS_NAMESPACE = "refs/exps"
EXPS_STASH = f"{EXPS_NAMESPACE}/stash"
EXEC_NAMESPACE = f"{EXPS_NAMESPACE}/exec"
EXEC_CHECKPOINT = f"{EXEC_NAMESPACE}/EXEC_CHECKPOINT"
EXEC_BRANCH = f"{EXEC_NAMESPACE}/EXEC_BRANCH"
EXEC_BASELINE = f"{EXEC_NAMESPACE}/EXEC_BASELINE"
EXEC_HEAD = f"{EXEC_NAMESPACE}/EXEC_HEAD"
EXEC_MERGE = f"{EXEC_NAMESPACE}/EXEC_MERGE"


class UnchangedExperimentError(DvcException):
    def __init__(self, rev):
        super().__init__(f"Experiment unchanged from '{rev[:7]}'.")
        self.rev = rev


class ExperimentExistsError(DvcException):
    def __init__(self, name: str):
        msg = (
            "Reproduced experiment conflicts with existing experiment "
            f"'{name}'. To overwrite the existing experiment run:\n\n"
            "\tdvc exp run -f ...\n\n"
            "To run this experiment with a different name run:\n\n"
            f"\tdvc exp run -n <new_name> ...\n"
        )
        super().__init__(msg)
        self.name = name


class CheckpointExistsError(DvcException):
    def __init__(self, name: str):
        msg = (
            "Reproduced checkpoint experiment conflicts with existing "
            f"experiment '{name}'. To restart (and overwrite) the existing "
            "experiment run:\n\n"
            "\tdvc exp run -f ...\n\n"
            "To resume the existing experiment, run:\n\n"
            f"\tdvc exp resume {name}\n"
        )
        super().__init__(msg)
        self.name = name


class InvalidExpRefError(DvcException):
    def __init__(self, ref):
        super().__init__(f"'{ref}' is not a valid experiment refname.")
        self.ref = ref


class ExpRefInfo:

    namespace = EXPS_NAMESPACE

    def __init__(
        self, baseline_sha: Optional[str] = None, name: Optional[str] = None,
    ):
        self.baseline_sha = baseline_sha
        self.name = name

    def __str__(self):
        return "/".join(self.parts)

    @property
    def parts(self):
        return (
            (self.namespace,)
            + (
                (self.baseline_sha[:2], self.baseline_sha[2:])
                if self.baseline_sha
                else ()
            )
            + ((self.name,) if self.name else ())
        )

    @classmethod
    def from_ref(cls, ref: str):
        try:
            parts = ref.split("/")
            if (
                len(parts) < 2
                or len(parts) == 3
                or len(parts) > 5
                or "/".join(parts[:2]) != EXPS_NAMESPACE
            ):
                InvalidExpRefError(ref)
        except ValueError:
            raise InvalidExpRefError(ref)
        baseline_sha = parts[2] + parts[3] if len(parts) >= 4 else None
        name = parts[4] if len(parts) == 5 else None
        return cls(baseline_sha, name)
