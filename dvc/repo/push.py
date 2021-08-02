from dvc.exceptions import FileTransferError, UploadError

from ..utils import glob_targets
from . import locked


@locked
def push(
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
    glob=False,
):
    used_run_cache = self.stage_cache.push(remote) if run_cache else []

    if isinstance(targets, str):
        targets = [targets]

    expanded_targets = glob_targets(targets, glob=glob)

    used = self.used_objs(
        expanded_targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        used_run_cache=used_run_cache,
        revs=revs,
    )

    pushed = len(used_run_cache)
    for odb, obj_ids in used.items():
        if odb and odb.read_only:
            continue
        try:
            pushed += self.cloud.push(obj_ids, jobs, remote=remote, odb=odb)
        except FileTransferError as exc:
            raise UploadError(exc.amount)
    return pushed
