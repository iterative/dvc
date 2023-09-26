from typing import Optional

from .exceptions import InvalidExpRefError

# Experiment refs are stored according baseline git SHA:
#   refs/exps/01/234abcd.../<exp_name>
EXPS_NAMESPACE = "refs/exps"
EXPS_STASH = f"{EXPS_NAMESPACE}/stash"
WORKSPACE_STASH = EXPS_STASH
APPLY_NAMESPACE = f"{EXPS_NAMESPACE}/apply"
APPLY_HEAD = f"{APPLY_NAMESPACE}/ORIG_HEAD"
APPLY_STASH = f"{APPLY_NAMESPACE}/stash"
CELERY_NAMESPACE = f"{EXPS_NAMESPACE}/celery"
CELERY_STASH = f"{CELERY_NAMESPACE}/stash"
CELERY_FAILED_STASH = f"{CELERY_NAMESPACE}/failed"
EXEC_NAMESPACE = f"{EXPS_NAMESPACE}/exec"
EXEC_APPLY = f"{EXEC_NAMESPACE}/EXEC_APPLY"
EXEC_BRANCH = f"{EXEC_NAMESPACE}/EXEC_BRANCH"
EXEC_BASELINE = f"{EXEC_NAMESPACE}/EXEC_BASELINE"
EXEC_HEAD = f"{EXEC_NAMESPACE}/EXEC_HEAD"
EXEC_MERGE = f"{EXEC_NAMESPACE}/EXEC_MERGE"
TEMP_NAMESPACE = f"{EXPS_NAMESPACE}/temp"
STASHES = {WORKSPACE_STASH, CELERY_STASH}
ITER_SKIP_NAMESPACES = {
    APPLY_NAMESPACE,
    CELERY_NAMESPACE,
    EXEC_NAMESPACE,
    TEMP_NAMESPACE,
}


class ExpRefInfo:
    namespace = EXPS_NAMESPACE

    def __init__(self, baseline_sha: str, name: Optional[str] = None):
        self.baseline_sha = baseline_sha
        self.name: str = name if name else ""

    def __str__(self):
        return "/".join(self.parts)

    def __repr__(self):
        baseline = f"'{self.baseline_sha}'"
        name = f"'{self.name}'" if self.name else "None"
        return f"ExpRefInfo(baseline_sha={baseline}, name={name})"

    @property
    def parts(self):
        return (
            (self.namespace,)
            + ((self.baseline_sha[:2], self.baseline_sha[2:]))
            + ((self.name,) if self.name else ())
        )

    @classmethod
    def from_ref(cls, ref: str):
        try:
            parts = ref.split("/")
            if (
                len(parts) < 4
                or len(parts) > 5
                or "/".join(parts[:2]) != EXPS_NAMESPACE
            ):
                raise InvalidExpRefError(ref)
        except ValueError:
            raise InvalidExpRefError(ref)  # noqa: B904
        baseline_sha = parts[2] + parts[3]
        name = parts[4] if len(parts) == 5 else None
        return cls(baseline_sha, name)
