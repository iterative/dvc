import logging
import os

from dvc.exceptions import PathMissingError
from dvc.repo import locked
from dvc.tree.local import LocalTree
from dvc.tree.repo import RepoTree

logger = logging.getLogger(__name__)


@locked
def diff(self, a_rev="HEAD", b_rev=None, targets=None):
    """
    By default, it compares the workspace with the last commit's tree.

    This implementation differs from `git diff` since DVC doesn't have
    the concept of `index`, but it keeps the same interface, thus,
    `dvc diff` would be the same as `dvc diff HEAD`.
    """

    if self.scm.no_commits:
        return {}

    b_rev = b_rev if b_rev else "workspace"
    results = {}
    missing_targets = {}
    for rev in self.brancher(revs=[a_rev, b_rev]):
        if rev == "workspace" and rev != b_rev:
            # brancher always returns workspace, but we only need to compute
            # workspace paths/checksums if b_rev was None
            continue

        targets_path_infos = None
        if targets is not None:
            # convert targets to path_infos, and capture any missing targets
            targets_path_infos, missing_targets[rev] = _targets_to_path_infos(
                self, targets
            )

        results[rev] = _paths_checksums(self, targets_path_infos)

    if targets is not None:
        # check for overlapping missing targets between a_rev and b_rev
        for target in set(missing_targets[a_rev]) & set(
            missing_targets[b_rev]
        ):
            raise PathMissingError(target, self)

    old = results[a_rev]
    new = results[b_rev]

    # Compare paths between the old and new tree.
    # set() efficiently converts dict keys to a set
    added = sorted(set(new) - set(old))
    deleted_or_missing = set(old) - set(new)
    if b_rev == "workspace":
        # missing status is only applicable when diffing local workspace
        # against a commit
        missing = sorted(_filter_missing(self, deleted_or_missing))
    else:
        missing = []
    deleted = sorted(deleted_or_missing - set(missing))
    modified = sorted(set(old) & set(new))

    ret = {
        "added": [{"path": path, "hash": new[path]} for path in added],
        "deleted": [{"path": path, "hash": old[path]} for path in deleted],
        "modified": [
            {"path": path, "hash": {"old": old[path], "new": new[path]}}
            for path in modified
            if old[path] != new[path]
        ],
        "not in cache": [
            {"path": path, "hash": old[path]} for path in missing
        ],
    }

    return ret if any(ret.values()) else {}


def _paths_checksums(repo, targets):
    """
    A dictionary of checksums addressed by relpaths collected from
    the current tree outputs.

    To help distinguish between a directory and a file output,
    the former one will come with a trailing slash in the path:

        directory: "data/"
        file:      "data"
    """

    return dict(_output_paths(repo, targets))


def _output_paths(repo, targets):
    repo_tree = RepoTree(repo, stream=True)
    on_working_tree = isinstance(repo.tree, LocalTree)

    def _exists(output):
        if on_working_tree:
            return output.exists
        return True

    def _to_path(output):
        return (
            str(output)
            if not output.is_dir_checksum
            else os.path.join(str(output), "")
        )

    def _to_checksum(output):
        if on_working_tree:
            return repo.cache.local.tree.get_hash(output.path_info).value
        return output.hash_info.value

    for stage in repo.stages:
        for output in stage.outs:
            if _exists(output):
                yield_output = targets is None or any(
                    output.path_info.isin_or_eq(target) for target in targets
                )

                if yield_output:
                    yield _to_path(output), _to_checksum(output)

                if output.is_dir_checksum and (
                    yield_output
                    or any(target.isin(output.path_info) for target in targets)
                ):
                    yield from _dir_output_paths(repo_tree, output, targets)


def _dir_output_paths(repo_tree, output, targets=None):
    from dvc.config import NoRemoteError

    try:
        for fname in repo_tree.walk_files(output.path_info):
            if targets is None or any(
                fname.isin_or_eq(target) for target in targets
            ):
                yield str(fname), repo_tree.get_file_hash(fname).value
    except NoRemoteError:
        logger.warning("dir cache entry for '%s' is missing", output)


def _filter_missing(repo, paths):
    repo_tree = RepoTree(repo, stream=True)
    for path in paths:
        metadata = repo_tree.metadata(path)
        if metadata.is_dvc:
            out = metadata.outs[0]
            if out.status().get(str(out)) == "not in cache":
                yield path


def _targets_to_path_infos(repo, targets):
    path_infos = []
    missing = []

    repo_tree = RepoTree(repo, stream=True)

    for target in targets:
        if repo_tree.exists(target):
            path_infos.append(repo_tree.metadata(target).path_info)
        else:
            missing.append(target)

    return path_infos, missing
