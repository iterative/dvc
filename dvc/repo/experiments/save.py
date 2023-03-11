import logging
import os
from typing import TYPE_CHECKING, List, Optional

from pathspec import PathSpec

from dvc.scm import Git

from .exceptions import ExperimentExistsError
from .refs import ExpRefInfo
from .utils import check_ref_format, get_random_exp_name

if TYPE_CHECKING:
    from dvc.repo import Repo


logger = logging.getLogger(__name__)


def _save_experiment(
    repo: "Repo",
    baseline_rev: str,
    force: bool,
    name: Optional[str],
    include_untracked: Optional[List[str]],
) -> str:
    repo.commit([], force=True, relink=False)

    name = name or get_random_exp_name(repo.scm, baseline_rev)
    ref_info = ExpRefInfo(baseline_rev, name)
    check_ref_format(repo.scm.dulwich, ref_info)
    ref = str(ref_info)
    if repo.scm.get_ref(ref) and not force:
        raise ExperimentExistsError(ref_info.name, command="save")

    assert isinstance(repo.scm, Git)

    repo.scm.add([], update=True)
    if include_untracked:
        repo.scm.add(include_untracked)
    repo.scm.commit(f"dvc: commit experiment {name}", no_verify=True)
    exp_rev = repo.scm.get_rev()
    repo.scm.set_ref(ref, exp_rev, old_ref=None)

    return exp_rev


def save(
    repo: "Repo",
    name: Optional[str] = None,
    force: bool = False,
    include_untracked: Optional[List[str]] = None,
) -> Optional[str]:
    """Save the current workspace status as an experiment.

    Returns the saved experiment's SHAs.
    """
    logger.debug("Saving workspace in %s", os.getcwd())

    assert isinstance(repo.scm, Git)

    _, _, untracked = repo.scm.status()
    if include_untracked:
        spec = PathSpec.from_lines("gitwildmatch", include_untracked)
        untracked = [file for file in untracked if not spec.match_file(file)]
    if untracked:
        logger.warning(
            (
                "The following untracked files were present in "
                "the workspace before saving but "
                "will not be included in the experiment commit:\n"
                "\t%s"
            ),
            ", ".join(untracked),
        )

    with repo.scm.detach_head(client="dvc") as orig_head:
        with repo.scm.stash_workspace() as workspace:
            try:
                if workspace is not None:
                    repo.scm.stash.apply(workspace)

                exp_rev = _save_experiment(
                    repo, orig_head, force, name, include_untracked
                )
            finally:
                repo.scm.reset(hard=True)

    return exp_rev
