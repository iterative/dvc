from __future__ import unicode_literals

import logging

from dvc.exceptions import CheckoutError
from dvc.exceptions import CheckoutErrorSuggestGit
from dvc.progress import Tqdm


logger = logging.getLogger(__name__)


def _cleanup_unused_links(repo):
    used = [
        out.fspath
        for stage in repo.stages
        for out in stage.outs
        if out.scheme == "local"
    ]
    repo.state.remove_unused_links(used)


def get_all_files_numbers(stages):
    return sum(stage.get_all_files_number() for stage in stages)


def _checkout(
    self,
    targets=None,
    with_deps=False,
    force=False,
    relink=False,
    recursive=False,
):
    from dvc.stage import StageFileDoesNotExistError, StageFileBadNameError

    stages = set()

    if not targets:
        targets = [None]
        _cleanup_unused_links(self)

    for target in targets:
        try:
            new = self.collect(
                target, with_deps=with_deps, recursive=recursive
            )
            stages.update(new)
        except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
            if not target:
                raise
            raise CheckoutErrorSuggestGit(target, exc)

    total = get_all_files_numbers(stages)
    if total == 0:
        logger.info("Nothing to do")
    failed = []
    with Tqdm(
        total=total, unit="file", desc="Checkout", disable=total == 0
    ) as pbar:
        for stage in stages:
            failed.extend(
                stage.checkout(
                    force=force,
                    progress_callback=pbar.update_desc,
                    relink=relink,
                )
            )
    if failed:
        raise CheckoutError(failed)
