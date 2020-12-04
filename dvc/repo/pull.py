import logging

from dvc.repo import locked

logger = logging.getLogger(__name__)


@locked
def pull(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    force=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
    glob=False,
):
    if isinstance(targets, str):
        targets = [targets]

    if glob:
        from glob import iglob

        expanded_targets = [
            exp_target
            for target in targets
            for exp_target in iglob(target, recursive=True)
        ]
    else:
        expanded_targets = targets

    processed_files_count = self.fetch(
        expanded_targets,
        jobs,
        remote=remote,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        recursive=recursive,
        run_cache=run_cache,
    )
    stats = self.checkout(
        targets=expanded_targets,
        with_deps=with_deps,
        force=force,
        recursive=recursive,
    )

    stats["fetched"] = processed_files_count
    return stats
