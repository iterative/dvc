import logging
import os
from typing import TYPE_CHECKING, List, Optional

from funcy import first

if TYPE_CHECKING:
    from dvc.repo import Repo


logger = logging.getLogger(__name__)


def save(
    repo: "Repo",
    name: Optional[str] = None,
    force: bool = False,
    include_untracked: Optional[List[str]] = None,
) -> Optional[str]:
    """Save the current workspace status as an experiment.

    Returns the saved experiment's SHAs.
    """
    queue = repo.experiments.workspace_queue
    logger.debug("Saving workspace in %s", os.getcwd())

    staged, _, _ = repo.scm.status(untracked_files="no")
    if staged:
        logger.warning(
            "Your workspace contains staged Git changes which will be "
            "unstaged before saving this experiment."
        )
        repo.scm.reset()

    entry = repo.experiments.new(queue=queue, name=name, force=force)
    executor = queue.init_executor(repo.experiments, entry)

    save_result = executor.save(
        executor.info, force=force, include_untracked=include_untracked
    )
    result = queue.collect_executor(repo.experiments, executor, save_result)

    exp_rev = first(result)
    return exp_rev
