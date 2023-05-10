import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Optional, Sequence

from dvc.config import NoRemoteError
from dvc.exceptions import DownloadError
from dvc.fs import Schemes

from . import locked

if TYPE_CHECKING:
    from dvc.data_cloud import Remote
    from dvc.repo import Repo
    from dvc.types import TargetType
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.transfer import TransferResult

logger = logging.getLogger(__name__)


@locked
def fetch(  # noqa: C901, PLR0913
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
    revs=None,
    odb: Optional["HashFileDB"] = None,
) -> int:
    """Download data items from a cloud and imported repositories

    Returns:
        int: number of successfully downloaded files

    Raises:
        DownloadError: thrown when there are failed downloads, either
            during `cloud.pull` or trying to fetch imported files

        config.NoRemoteError: thrown when downloading only local files and no
            remote is configured
    """
    from dvc.repo.imports import save_imports
    from dvc_data.hashfile.transfer import TransferResult

    if isinstance(targets, str):
        targets = [targets]

    worktree_remote: Optional["Remote"] = None
    with suppress(NoRemoteError):
        _remote = self.cloud.get_remote(name=remote)
        if _remote.worktree or _remote.fs.version_aware:
            worktree_remote = _remote

    failed_count = 0
    transferred_count = 0

    try:
        if run_cache:
            self.stage_cache.pull(remote)
    except DownloadError as exc:
        failed_count += exc.amount

    no_remote_msg: Optional[str] = None
    result = TransferResult(set(), set())
    try:
        if worktree_remote is not None:
            transferred_count += _fetch_worktree(
                self,
                worktree_remote,
                revs=revs,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
                targets=targets,
                jobs=jobs,
                with_deps=with_deps,
                recursive=recursive,
            )
        else:
            d, f = _fetch(
                self,
                targets,
                all_branches=all_branches,
                all_tags=all_tags,
                all_commits=all_commits,
                with_deps=with_deps,
                force=True,
                remote=remote,
                jobs=jobs,
                recursive=recursive,
                revs=revs,
                odb=odb,
            )
            result.transferred.update(d)
            result.failed.update(f)
    except NoRemoteError as exc:
        no_remote_msg = str(exc)

    for rev in self.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        imported = save_imports(
            self,
            targets,
            unpartial=not rev or rev == "workspace",
            recursive=recursive,
        )
        result.transferred.update(imported)
        result.failed.difference_update(imported)

    failed_count += len(result.failed)

    if failed_count:
        if no_remote_msg:
            logger.error(no_remote_msg)
        raise DownloadError(failed_count)

    transferred_count += len(result.transferred)
    return transferred_count


def _fetch(
    repo: "Repo",
    targets: "TargetType",
    remote: Optional[str] = None,
    jobs: Optional[int] = None,
    odb: Optional["HashFileDB"] = None,
    **kwargs,
) -> "TransferResult":
    from dvc_data.hashfile.transfer import TransferResult

    result = TransferResult(set(), set())
    used = repo.used_objs(
        targets,
        remote=remote,
        jobs=jobs,
        **kwargs,
    )
    if odb:
        all_ids = set()
        for _odb, obj_ids in used.items():
            all_ids.update(obj_ids)
        d, f = repo.cloud.pull(
            all_ids,
            jobs=jobs,
            remote=remote,
            odb=odb,
        )
        result.transferred.update(d)
        result.failed.update(f)
    else:
        for src_odb, obj_ids in sorted(
            used.items(),
            key=lambda item: item[0] is not None
            and item[0].fs.protocol == Schemes.MEMORY,
        ):
            d, f = repo.cloud.pull(
                obj_ids,
                jobs=jobs,
                remote=remote,
                odb=src_odb,
            )
            result.transferred.update(d)
            result.failed.update(f)
    return result


def _fetch_worktree(
    repo: "Repo",
    remote: "Remote",
    revs: Optional[Sequence[str]] = None,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    targets: Optional["TargetType"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> int:
    from dvc.repo.worktree import fetch_worktree

    downloaded = 0
    for _ in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        downloaded += fetch_worktree(repo, remote, targets=targets, jobs=jobs, **kwargs)
    return downloaded
