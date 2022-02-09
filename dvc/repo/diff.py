import logging
import os
from collections import defaultdict
from typing import Dict, List

from dvc.exceptions import PathMissingError
from dvc.repo import locked

logger = logging.getLogger(__name__)


@locked
def diff(self, a_rev="HEAD", b_rev=None, targets=None):
    """
    By default, it compares the workspace with the last commit's fs.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """

    if self.scm.no_commits:
        return {}

    from dvc.fs.repo import RepoFileSystem

    repo_fs = RepoFileSystem(self)

    b_rev = b_rev if b_rev else "workspace"
    results = {}
    missing_targets = {}
    for rev in self.brancher(revs=[a_rev, b_rev]):
        if rev == "workspace" and rev != b_rev:
            # brancher always returns workspace, but we only need to compute
            # workspace paths/checksums if b_rev was None
            continue

        targets_paths = None
        if targets is not None:
            # convert targets to paths, and capture any missing targets
            targets_paths, missing_targets[rev] = _targets_to_paths(
                repo_fs, targets
            )

        results[rev] = _paths_checksums(self, targets_paths)

    if targets is not None:
        # check for overlapping missing targets between a_rev and b_rev
        for target in set(missing_targets[a_rev]) & set(
            missing_targets[b_rev]
        ):
            raise PathMissingError(target, self)

    old = results[a_rev]
    new = results[b_rev]

    # Compare paths between the old and new fs.
    # set() efficiently converts dict keys to a set
    added = sorted(set(new) - set(old))
    deleted_or_missing = set(old) - set(new)
    if b_rev == "workspace":
        # missing status is only applicable when diffing local workspace
        # against a commit
        missing = sorted(_filter_missing(repo_fs, deleted_or_missing))
    else:
        missing = []
    deleted = sorted(deleted_or_missing - set(missing))
    modified = sorted(set(old) & set(new))

    # Cases when file was changed and renamed are resulted
    # in having deleted and added record
    # To cover such cases we need to change hashing function
    # to produce rolling/chunking hash

    renamed = _calculate_renamed(new, old, added, deleted)

    for renamed_item in renamed:
        added.remove(renamed_item["path"]["new"])
        deleted.remove(renamed_item["path"]["old"])

    ret = {
        "added": [{"path": path, "hash": new[path]} for path in added],
        "deleted": [{"path": path, "hash": old[path]} for path in deleted],
        "modified": [
            {"path": path, "hash": {"old": old[path], "new": new[path]}}
            for path in modified
            if old[path] != new[path]
        ],
        "renamed": renamed,
        "not in cache": [
            {"path": path, "hash": old[path]} for path in missing
        ],
    }

    return ret if any(ret.values()) else {}


def _paths_checksums(repo, targets):
    """
    A dictionary of checksums addressed by relpaths collected from
    the current fs outputs.

    To help distinguish between a directory and a file output,
    the former one will come with a trailing slash in the path:

        directory: "data/"
        file:      "data"
    """

    return dict(_output_paths(repo, targets))


def _output_paths(repo, targets):
    from dvc.data.stage import stage as ostage
    from dvc.fs.local import LocalFileSystem

    on_working_fs = isinstance(repo.fs, LocalFileSystem)

    def _exists(output):
        if on_working_fs:
            return output.exists
        return True

    def _to_path(output):
        return (
            str(output)
            if not output.is_dir_checksum
            else os.path.join(str(output), "")
        )

    for output in repo.index.outs:
        if _exists(output):
            yield_output = targets is None or any(
                output.fs.path.isin_or_eq(output.fs_path, target)
                for target in targets
            )

            if on_working_fs:
                _, _, obj = ostage(
                    repo.odb.local,
                    output.fs_path,
                    repo.odb.local.fs,
                    "md5",
                    dry_run=True,
                    dvcignore=output.dvcignore,
                )
                hash_info = obj.hash_info
            else:
                hash_info = output.hash_info
                obj = output.get_obj()

            if yield_output:
                yield _to_path(output), hash_info.value

            if not obj:
                continue

            if output.is_dir_checksum and (
                yield_output
                or any(
                    output.fs.path.isin(target, output.fs_path)
                    for target in targets
                )
            ):
                yield from _dir_output_paths(
                    output.fs, output.fs_path, obj, targets
                )


def _dir_output_paths(fs, fs_path, obj, targets=None):
    if fs.scheme == "local":
        # NOTE: workaround for filesystems that are based on full local paths
        # (e.g. gitfs, dvcfs, repofs). Proper solution is to use upcoming
        # fsspec's prefixfs to use relpaths as fs_paths.
        base = fs.path.relpath(fs_path, os.getcwd())
    else:
        base = fs_path
    for key, _, oid in obj:
        fname = fs.path.join(fs_path, *key)
        if targets is None or any(
            fs.path.isin_or_eq(fname, target) for target in targets
        ):
            # pylint: disable=no-member
            yield fs.path.join(base, *key), oid.value


def _filter_missing(repo_fs, paths):
    for path in paths:
        try:
            info = repo_fs.info(path)
            if (
                info["isdvc"]
                and info["type"] == "directory"
                and not info["meta"].obj
            ):
                yield path
        except FileNotFoundError:
            pass


def _targets_to_paths(repo_fs, targets):
    paths = []
    missing = []

    for target in targets:
        if repo_fs.exists(target):
            paths.append(os.path.abspath(target))
        else:
            missing.append(target)

    return paths, missing


def _calculate_renamed(new, old, added, deleted):
    old_inverted: Dict[str, List[str]] = defaultdict(list)
    # It is needed to be dict of lists to cover cases
    # when repo has paths with same hash
    for path, path_hash in old.items():
        old_inverted[path_hash].append(path)

    renamed = []
    for path in added:
        path_hash = new[path]
        old_paths = old_inverted[path_hash]
        try:
            iterator = enumerate(old_paths)
            index = next(idx for idx, path in iterator if path in deleted)
        except StopIteration:
            continue

        old_path = old_paths.pop(index)
        renamed.append(
            {"path": {"old": old_path, "new": path}, "hash": path_hash}
        )

    return renamed
