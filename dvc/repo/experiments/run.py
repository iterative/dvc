import logging
from typing import Dict, Iterable, Optional

from dvc.dependency.param import ParamsDependency
from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.ui import ui
from dvc.utils.cli_parse import to_path_overrides

logger = logging.getLogger(__name__)


@locked
def run(  # noqa: C901, PLR0912
    repo,
    targets: Optional[Iterable[str]] = None,
    params: Optional[Iterable[str]] = None,
    run_all: bool = False,
    jobs: int = 1,
    tmp_dir: bool = False,
    queue: bool = False,
    copy_paths: Optional[Iterable[str]] = None,
    message: Optional[str] = None,
    **kwargs,
) -> Dict[str, str]:
    """Reproduce the specified targets as an experiment.

    Accepts the same additional kwargs as Repo.reproduce.

    Returns a dict mapping new experiment SHAs to the results
    of `repro` for that experiment.
    """
    if run_all:
        return repo.experiments.reproduce_celery(jobs=jobs)

    hydra_sweep = None
    if params:
        from dvc.utils.hydra import to_hydra_overrides

        path_overrides = to_path_overrides(params)

        if tmp_dir or queue:
            untracked = repo.scm.untracked_files()
            for path in path_overrides:
                if path in untracked:
                    logger.debug(
                        "'%s' is currently untracked but will be modified by DVC. "
                        "Adding it to git.",
                        path,
                    )
                    repo.scm.add([path])

        hydra_sweep = any(
            x.is_sweep_override()
            for param_file in path_overrides
            for x in to_hydra_overrides(path_overrides[param_file])
        )

        if hydra_sweep and not queue:
            raise InvalidArgumentError(
                "Sweep overrides can't be used without `--queue`"
            )
    else:
        path_overrides = {}

    hydra_enabled = repo.config.get("hydra", {}).get("enabled", False)
    hydra_output_file = ParamsDependency.DEFAULT_PARAMS_FILE
    if hydra_enabled and hydra_output_file not in path_overrides:
        # Force `_update_params` even if `--set-param` was not used
        path_overrides[hydra_output_file] = []

    if not queue:
        return repo.experiments.reproduce_one(
            targets=targets,
            params=path_overrides,
            tmp_dir=tmp_dir,
            copy_paths=copy_paths,
            message=message,
            **kwargs,
        )

    if hydra_sweep:
        from dvc.utils.hydra import get_hydra_sweeps

        sweeps = get_hydra_sweeps(path_overrides)
        name_prefix = kwargs.get("name")
    else:
        sweeps = [path_overrides]

    for idx, sweep_overrides in enumerate(sweeps):
        if hydra_sweep and name_prefix is not None:
            kwargs["name"] = f"{name_prefix}-{idx+1}"
        queue_entry = repo.experiments.queue_one(
            repo.experiments.celery_queue,
            targets=targets,
            params=sweep_overrides,
            copy_paths=copy_paths,
            message=message,
            **kwargs,
        )
        if sweep_overrides:
            ui.write(f"Queueing with overrides '{sweep_overrides}'.")
        name = queue_entry.name or queue_entry.stash_rev[:7]
        ui.write(f"Queued experiment '{name}' for future execution.")

    return {}
