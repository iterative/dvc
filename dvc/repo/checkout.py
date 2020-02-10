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

    unused = repo.state.get_unused_links(used)
    for link in unused:
        logger.info(
            "Removing '{}' as it already exists in the current worktree.", link
        )
    repo.state.remove_links(unused)
    return bool(unused)


def get_all_files_numbers(pairs):
    return sum(
        stage.get_all_files_number(filter_info) for stage, filter_info in pairs
    )


def _checkout(
    self,
    targets=None,
    with_deps=False,
    force=False,
    relink=False,
    recursive=False,
):
    from dvc.stage import StageFileDoesNotExistError, StageFileBadNameError

    cleaned = False
    if not targets:
        targets = [None]
        cleaned = _cleanup_unused_links(self)

    pairs = set()
    for target in targets:
        try:
            pairs.update(
                self.collect_granular(
                    target, with_deps=with_deps, recursive=recursive
                )
            )
        except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
            if not target:
                raise
            raise CheckoutErrorSuggestGit(target) from exc

    total = get_all_files_numbers(pairs)
    if total == 0 and not cleaned:
        logger.info("Nothing to do")
    failed = []
    with Tqdm(
        total=total, unit="file", desc="Checkout", disable=total == 0
    ) as pbar:
        for stage, filter_info in pairs:
            failed.extend(
                stage.checkout(
                    force=force,
                    progress_callback=pbar.update_desc,
                    relink=relink,
                    filter_info=filter_info,
                )
            )
    if failed:
        raise CheckoutError(failed)
