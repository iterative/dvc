from typing import TYPE_CHECKING

from dvc.exceptions import InvalidArgumentError

from . import locked

if TYPE_CHECKING:
    from dvc.repo.stage import StageInfo


@locked
def update(  # noqa: C901
    self,
    targets=None,
    rev=None,
    recursive=False,
    to_remote=False,
    no_download=False,
    remote=None,
    jobs=None,
):
    from .worktree import update_worktree_stages

    if not targets:
        targets = [None]

    if isinstance(targets, str):
        targets = [targets]

    if to_remote and no_download:
        raise InvalidArgumentError("--to-remote can't be used with --no-download")

    if not to_remote and remote:
        raise InvalidArgumentError("--remote can't be used without --to-remote")

    import_stages = set()
    other_stage_infos: list[StageInfo] = []

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
        stage.dump()

    if other_stage_infos:
        if rev:
            raise InvalidArgumentError("--rev can't be used with worktree update")
        if no_download:
            raise InvalidArgumentError(
                "--no-download can't be used with worktree update"
            )
        if to_remote:
            raise InvalidArgumentError("--to-remote can't be used with worktree update")
        update_worktree_stages(self, other_stage_infos)

    stages = import_stages | {stage_info.stage for stage_info in other_stage_infos}
    return list(stages)
