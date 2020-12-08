import logging

from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .utils import remote_exp_refs_by_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def pull(repo, git_remote, exp_name, *args, force=False, **kwargs):
    exp_ref = _get_exp_ref(repo, git_remote, exp_name)

    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the local experiment "
            "re-run with '--force'."
        )

    refspec = f"{exp_ref}:{exp_ref}"
    logger.debug("Pull '%s' -> '%s'", git_remote, refspec)
    repo.scm.fetch_refspecs(
        git_remote, [refspec], force=force, on_diverged=on_diverged
    )


def _get_exp_ref(repo, git_remote, exp_name):
    if exp_name.startswith("refs/"):
        return exp_name

    exp_refs = list(remote_exp_refs_by_name(repo.scm, git_remote, exp_name))
    if not exp_refs:
        raise InvalidArgumentError(
            f"Experiment '{exp_name}' does not exist in '{git_remote}'"
        )
    if len(exp_refs) > 1:
        cur_rev = repo.scm.get_rev()
        for info in exp_refs:
            if info.baseline_sha == cur_rev:
                return info
        msg = [
            (
                f"Ambiguous name '{exp_name}' refers to multiple "
                "experiments in '{git_remote}'. Use full refname to pull one "
                "of the following:"
            ),
            "",
        ]
        msg.extend([f"\t{info}" for info in exp_refs])
        raise InvalidArgumentError("\n".join(msg))
    return exp_refs[0]
