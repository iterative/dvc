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
):
    processed_files_count = self._fetch(
        targets,
        jobs,
        remote=remote,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        recursive=recursive,
    )
    stats = self._checkout(
        targets=targets, with_deps=with_deps, force=force, recursive=recursive
    )

    stats["downloaded"] = processed_files_count
    return stats
