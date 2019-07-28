from __future__ import unicode_literals

import logging

from dvc.exceptions import CheckoutErrorSuggestGit
from dvc.progress import Tqdm


logger = logging.getLogger(__name__)


def _cleanup_unused_links(self, all_stages):
    used = [
        out.fspath
        for stage in all_stages
        for out in stage.outs
        if out.scheme == "local"
    ]
    self.state.remove_unused_links(used)


def get_all_files_numbers(stages):
    return sum(stage.get_all_files_number() for stage in stages)


def checkout(self, target=None, with_deps=False, force=False, recursive=False):
    from dvc.stage import StageFileDoesNotExistError, StageFileBadNameError

    all_stages = self.stages()

    try:
        stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
        if not target:
            raise
        raise CheckoutErrorSuggestGit(target, exc)

    with self.state:
        _cleanup_unused_links(self, all_stages)
        pbar = Tqdm(total=get_all_files_numbers(stages))

        for stage in stages:
            if stage.locked:
                logger.warning(
                    "DVC-file '{path}' is locked. Its dependencies are"
                    " not going to be checked out.".format(path=stage.relpath)
                )

            stage.checkout(force=force, progress_callback=pbar.update_desc)
        pbar.close()
