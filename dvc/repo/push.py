from typing import TYPE_CHECKING, Optional

from dvc.exceptions import FileTransferError, UploadError

from ..utils import glob_targets
from . import locked

if TYPE_CHECKING:
    from dvc_objects.db.base import ObjectDB


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
    odb: Optional["ObjectDB"] = None,
    include_imports=False,
):
    used_run_cache = (
        self.stage_cache.push(remote, odb=odb) if run_cache else []
    )

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
    if odb:
        all_ids = set()
        for dest_odb, obj_ids in used.items():
            if not include_imports and dest_odb and dest_odb.read_only:
                continue
            all_ids.update(obj_ids)
        try:
            pushed += self.cloud.push(all_ids, jobs, remote=remote, odb=odb)
        except FileTransferError as exc:
            raise UploadError(exc.amount)
    else:
        for dest_odb, obj_ids in used.items():
            if dest_odb and dest_odb.read_only:
                continue
            try:
                pushed += self.cloud.push(
                    obj_ids, jobs, remote=remote, odb=odb or dest_odb
                )
            except FileTransferError as exc:
                raise UploadError(exc.amount)
    return pushed
