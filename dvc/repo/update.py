from typing import TYPE_CHECKING, List

from dvc.exceptions import InvalidArgumentError
from dvc.stage.exceptions import StageUpdateError

from . import locked

if TYPE_CHECKING:
    from dvc.repo.stage import StageInfo


@locked
def update(
    self,
    targets=None,
    rev=None,
    recursive=False,
    to_remote=False,
    no_download=False,
    remote=None,
    jobs=None,
):
    from ..dvcfile import Dvcfile
    from .worktree import update_worktree_stages

    if not targets:
        targets = [None]

    if isinstance(targets, str):
        targets = [targets]

    if to_remote and no_download:
        raise InvalidArgumentError(
            "--to-remote can't be used with --no-download"
        )

    if not to_remote and remote:
        if not self.cloud.get_remote(name=remote).worktree:
            raise InvalidArgumentError(
                "--remote can't be used without --to-remote"
            )

    import_stages = set()
    other_stage_infos: List["StageInfo"] = []

    for stage_info in self.index.collect_targets(targets, recursive=recursive):
        if stage_info.stage.is_import:
            import_stages.add(stage_info.stage)
        else:
            other_stage_infos.append(stage_info)

    for stage in import_stages:
        stage.update(
            rev,
            to_remote=to_remote,
            remote=remote,
            no_download=no_download,
            jobs=jobs,
        )
        dvcfile = Dvcfile(self, stage.path)
        dvcfile.dump(stage)

    if other_stage_infos:
        remote_obj = self.cloud.get_remote(name=remote)
        if not remote_obj.worktree:
            raise StageUpdateError(other_stage_infos[0].stage.relpath)
        if rev:
            raise InvalidArgumentError(
                "--rev can't be used with worktree update"
            )
        if no_download:
            raise InvalidArgumentError(
                "--no-download can't be used with worktree update"
            )
        if to_remote:
            raise InvalidArgumentError(
                "--to-remote can't be used with worktree update"
            )
        update_worktree_stages(
            self,
            other_stage_infos,
            remote_obj,
        )

    stages = import_stages | {
        stage_info.stage for stage_info in other_stage_infos
    }
    return list(stages)
