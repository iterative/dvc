from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.progress import progress


def _cleanup_unused_links(self, all_stages):
    used = []
    for stage in all_stages:
        for out in stage.outs:
            used.append(out.path)
    self.state.remove_unused_links(used)


def checkout(self, target=None, with_deps=False, force=False, recursive=False):
    from dvc.stage import StageFileDoesNotExistError, StageFileBadNameError

    if target and not recursive:
        all_stages = self.active_stages()
        try:
            stages = self.collect(target, with_deps=with_deps)
        except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
            raise DvcException(
                str(exc) + " Did you mean 'git checkout {}'?".format(target)
            )
    else:
        all_stages = self.active_stages(target)
        stages = all_stages

    with self.state:
        _cleanup_unused_links(self, all_stages)

        checkout_progress_message = "Checkout in progress"
        for index, stage in enumerate(stages):
            if stage.locked:
                logger.warning(
                    "DVC file '{path}' is locked. Its dependencies are"
                    " not going to be checked out.".format(path=stage.relpath)
                )

            stage.checkout(force=force)
            progress.update_target(
                checkout_progress_message, index + 1, len(stages)
            )
            import time

            time.sleep(1)
        progress.finish_target(checkout_progress_message)
