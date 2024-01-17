import os
from typing import TYPE_CHECKING, Iterable, List, Optional

from funcy import first

from dvc.log import logger

if TYPE_CHECKING:
    from dvc.repo import Repo


logger = logger.getChild(__name__)


def save(
    repo: "Repo",
    targets: Optional[Iterable[str]] = None,
    name: Optional[str] = None,
    force: bool = False,
    include_untracked: Optional[List[str]] = None,
    message: Optional[str] = None,
) -> Optional[str]:
    """Save the current workspace status as an experiment.

    Returns the saved experiment's SHAs.
    """
    logger.debug("Saving workspace in %s", os.getcwd())

    queue = repo.experiments.workspace_queue
    entry = repo.experiments.new(queue=queue, name=name, force=force)
    executor = queue.init_executor(repo.experiments, entry)

    try:
        save_result = executor.save(
            executor.info,
            targets=targets,
            force=force,
            include_untracked=include_untracked,
            message=message,
        )
        result = queue.collect_executor(repo.experiments, executor, save_result)
    finally:
        executor.cleanup()

    return first(result)
