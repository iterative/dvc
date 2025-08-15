from dvc.exceptions import CheckoutError
from dvc.log import logger
from dvc.repo import locked
from dvc.utils import glob_targets

logger = logger.getChild(__name__)


@locked
def pull(  # noqa: PLR0913
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
    allow_missing=False,
):
    if isinstance(targets, str):
        targets = [targets]

    expanded_targets = glob_targets(targets, glob=glob)

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
    try:
        result = self.checkout(
            targets=expanded_targets,
            with_deps=with_deps,
            force=force,
            recursive=recursive,
            allow_missing=allow_missing,
        )
    except CheckoutError as exc:
        # put fetched counts first
        exc.result["stats"] = {"fetched": processed_files_count} | exc.result["stats"]
        raise
    else:
        # put fetched counts first
        result["stats"] = {"fetched": processed_files_count} | result["stats"]
    return result
