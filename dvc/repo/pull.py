from __future__ import unicode_literals

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
):
    processed_files_count = self._fetch(
        targets,
        jobs,
        remote=remote,
        all_branches=all_branches,
        with_deps=with_deps,
        all_tags=all_tags,
        recursive=recursive,
    )
    self._checkout(
        targets=targets,
        with_deps=with_deps,
        force=force,
        recursive=recursive,

    )
    logger.info("Data retrieved successfully from DVC remote storage.")
    return processed_files_count
