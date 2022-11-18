import logging
from itertools import compress

from funcy.py3 import cat

from dvc.exceptions import InvalidArgumentError

from . import locked

logger = logging.getLogger(__name__)


def _joint_status(pairs):
    status_info = {}

    for stage, filter_info in pairs:
        if stage.frozen and not (
            stage.is_repo_import or stage.is_versioned_import
        ):
            logger.warning(
                "%s is frozen. Its dependencies are"
                " not going to be shown in the status output.",
                stage,
            )
        status_info.update(
            stage.status(check_updates=True, filter_info=filter_info)
        )

    return status_info


def _local_status(self, targets=None, with_deps=False, recursive=False):
    targets = targets or [None]
    pairs = cat(
        self.stage.collect_granular(
            t, with_deps=with_deps, recursive=recursive
        )
        for t in targets
    )

    return _joint_status(pairs)


def _cloud_status(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
):
    """Returns a dictionary with the files that are new or deleted.

    - new: Remote doesn't have the file
    - deleted: File is no longer in the local cache
    - missing: File doesn't exist neither in the cache, neither in remote

    Example:
            Given the following commands:

            $ echo "foo" > foo
            $ echo "bar" > bar
            $ dvc add foo bar
            $ dvc status -c

            It will return something like:

            { "foo": "new", "bar": "new" }

            Now, after pushing and removing "bar" from the local cache:

            $ dvc push
            $ rm .dvc/cache/c1/57a79031e1c40f85931829bc5fc552

            The result would be:

            { "bar": "deleted" }
    """
    used = self.used_objs(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        push=True,
    )

    ret = {}
    for odb, obj_ids in used.items():
        if odb is not None:
            # ignore imported objects
            continue
        status_info = self.cloud.status(
            obj_ids, jobs, remote=remote, log_missing=False
        )
        for status_ in ("deleted", "new", "missing"):
            for hash_info in getattr(status_info, status_, []):
                ret[hash_info.obj_name] = status_

    return ret


@locked
def status(
    self,
    targets=None,
    jobs=None,
    cloud=False,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    all_commits=False,
    recursive=False,
):
    if isinstance(targets, str):
        targets = [targets]

    if cloud or remote:
        return _cloud_status(
            self,
            targets,
            jobs,
            all_branches=all_branches,
            with_deps=with_deps,
            remote=remote,
            all_tags=all_tags,
            all_commits=all_commits,
            recursive=True,
        )

    ignored = list(
        compress(
            ["--all-branches", "--all-tags", "--all-commits", "--jobs"],
            [all_branches, all_tags, all_commits, jobs],
        )
    )
    if ignored:
        msg = "The following options are meaningless for local status: {}"
        raise InvalidArgumentError(msg.format(", ".join(ignored)))

    return _local_status(
        self, targets, with_deps=with_deps, recursive=recursive
    )
