import logging
from typing import Dict, Iterable, Optional

from dvc.repo import locked
from dvc.ui import ui
from dvc.utils.cli_parse import to_path_overrides

logger = logging.getLogger(__name__)


@locked
def run(
    repo,
    targets: Optional[Iterable[str]] = None,
    params: Optional[Iterable[str]] = None,
    run_all: bool = False,
    jobs: int = 1,
    tmp_dir: bool = False,
    queue: bool = False,
    **kwargs,
) -> Dict[str, str]:
    """Reproduce the specified targets as an experiment.

    Accepts the same additional kwargs as Repo.reproduce.

    Returns a dict mapping new experiment SHAs to the results
    of `repro` for that experiment.
    """
    if run_all:
        entries = list(repo.experiments.celery_queue.iter_queued())
        return repo.experiments.reproduce_celery(entries, jobs=jobs)

    if params:
        params = to_path_overrides(params)

    if queue:
        if not kwargs.get("checkpoint_resume", None):
            kwargs["reset"] = True
        queue_entry = repo.experiments.queue_one(
            repo.experiments.celery_queue,
            targets=targets,
            params=params,
            **kwargs,
        )
        name = queue_entry.name or queue_entry.stash_rev[:7]
        ui.write(f"Queued experiment '{name}' for future execution.")
        return {}

    return repo.experiments.reproduce_one(
        targets=targets, params=params, tmp_dir=tmp_dir, **kwargs
    )
