from typing import Optional

from dvc.exceptions import DvcException
from dvc.scm import SCM

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
        super().__init__(f"Experiment identical to baseline '{rev[:7]}'.")
        self.rev = rev


class CheckpointExistsError(DvcException):
    def __init__(self, rev: str, continue_rev: Optional[str] = None):
        if not continue_rev:
            continue_rev = rev
        msg = (
            f"Checkpoint experiment containing '{rev[:7]}' already exists."
            " To restart the experiment run:\n\n"
            "\tdvc exp run -f ...\n\n"
            "To resume the experiment, run:\n\n"
            f"\tdvc exp resume {continue_rev[:7]}\n"
        )
        super().__init__(msg)
        self.rev = rev


def split_exps_refname(refname):
    """Return (namespace, sha, name) ref name tuple."""
    refs, namespace, sha1, sha2, name = refname.split("/", maxsplit=4)
    return "/".join([refs, namespace]), sha1 + sha2, name


def get_exps_refname(scm: SCM, baseline: str, name: Optional[str] = None):
    """Return git ref name for the specified experiment.

    Args:
        scm: SCM instance
        baseline: baseline git commit SHA (or named ref)
        name: experiment name
    """
    sha = scm.resolve_rev(baseline)
    parts = [EXPS_NAMESPACE, sha[:2], sha[2:]]
    if name is not None:
        parts.append(name)
    return "/".join(parts)
