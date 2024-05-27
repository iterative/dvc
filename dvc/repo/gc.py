from typing import TYPE_CHECKING, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.log import logger

from . import locked

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.index import ObjectContainer

logger = logger.getChild(__name__)


def _validate_args(**kwargs):
    not_in_remote = kwargs.pop("not_in_remote", None)
    cloud = kwargs.pop("cloud", None)
    remote = kwargs.pop("remote", None)
    if remote and not (cloud or not_in_remote):
        raise InvalidArgumentError("`--remote` requires `--cloud` or `--not-in-remote`")
    if not_in_remote and cloud:
        raise InvalidArgumentError(
            "`--not-in-remote` and `--cloud` are mutually exclusive"
        )
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
            "needs to be set."
        )
    if kwargs.get("num") and not kwargs.get("rev"):
        raise InvalidArgumentError("`--num` can only be used alongside `--rev`")


def _used_obj_ids_not_in_remote(
    remote_odb_to_obj_ids: "ObjectContainer", jobs: Optional[int] = None
):
    used_obj_ids = set()
    remote_oids = set()
    for remote_odb, obj_ids in remote_odb_to_obj_ids.items():
        assert remote_odb
        remote_oids.update(
            remote_odb.list_oids_exists(
                {x.value for x in obj_ids if x.value},
                jobs=jobs,
            )
        )
        used_obj_ids.update(obj_ids)
    return {obj for obj in used_obj_ids if obj.value not in remote_oids}


@locked
def gc(  # noqa: C901, PLR0912, PLR0913
    self: "Repo",
    all_branches: bool = False,
    cloud: bool = False,
    remote: Optional[str] = None,
    with_deps: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    all_experiments: bool = False,
    force: bool = False,
    jobs: Optional[int] = None,
    repos: Optional[list[str]] = None,
    workspace: bool = False,
    commit_date: Optional[str] = None,
    rev: Optional[str] = None,
    num: Optional[int] = None,
    not_in_remote: bool = False,
    dry: bool = False,
    skip_failed: bool = False,
):
    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # `all_experiments` or `all_branches` are enabled.
    _validate_args(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        all_experiments=all_experiments,
        commit_date=commit_date,
        rev=rev,
        num=num,
        cloud=cloud,
        not_in_remote=not_in_remote,
    )

    from contextlib import ExitStack

    from dvc.repo import Repo
    from dvc_data.hashfile.db import get_index
    from dvc_data.hashfile.gc import gc as ogc

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    odb_to_obj_ids: ObjectContainer = {}
    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in [*all_repos, self]:
            for odb, obj_ids in repo.used_objs(
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
                all_commits=all_commits,
                all_experiments=all_experiments,
                commit_date=commit_date,
                remote=remote,
                force=force,
                jobs=jobs,
                revs=[rev] if rev else None,
                num=num or 1,
                skip_failed=skip_failed,
            ).items():
                if odb not in odb_to_obj_ids:
                    odb_to_obj_ids[odb] = set()
                odb_to_obj_ids[odb].update(obj_ids)

    if cloud or not_in_remote:
        _merge_remote_obj_ids(self, remote, odb_to_obj_ids)
    if not_in_remote:
        used_obj_ids = _used_obj_ids_not_in_remote(odb_to_obj_ids, jobs=jobs)
    else:
        used_obj_ids = set()
        used_obj_ids.update(*odb_to_obj_ids.values())

    for scheme, odb in self.cache.by_scheme():
        if not odb:
            continue
        num_removed = ogc(odb, used_obj_ids, jobs=jobs, dry=dry)
        if num_removed:
            logger.info("Removed %d objects from %s cache.", num_removed, scheme)
        else:
            logger.info("No unused '%s' cache to remove.", scheme)

    if not cloud:
        return

    for remote_odb, obj_ids in odb_to_obj_ids.items():
        assert remote_odb is not None
        num_removed = ogc(remote_odb, obj_ids, jobs=jobs, dry=dry)
        if num_removed:
            get_index(remote_odb).clear()
            logger.info("Removed %d objects from remote.", num_removed)
        else:
            logger.info("No unused cache to remove from remote.")


def _merge_remote_obj_ids(
    repo: "Repo", remote: Optional[str], used_objs: "ObjectContainer"
):
    # Merge default remote used objects with remote-per-output used objects
    default_obj_ids = used_objs.pop(None, set())
    remote_odb = repo.cloud.get_remote_odb(remote, "gc -c", hash_name="md5")
    if remote_odb not in used_objs:
        used_objs[remote_odb] = set()
    used_objs[remote_odb].update(default_obj_ids)
    legacy_odb = repo.cloud.get_remote_odb(remote, "gc -c", hash_name="md5-dos2unix")
    if legacy_odb not in used_objs:
        used_objs[legacy_odb] = set()
    used_objs[legacy_odb].update(default_obj_ids)
