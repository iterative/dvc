from dvc.exceptions import InvalidArgumentError

from . import locked


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
    all_=None,
):
    from ..dvcfile import Dvcfile

    if targets and all_:
        raise InvalidArgumentError("Can't use targets with --all")

    if not targets:
        targets = [None]

    if isinstance(targets, str):
        targets = [targets]

    if to_remote and no_download:
        raise InvalidArgumentError(
            "--to-remote can't be used with --no-download"
        )

    if not to_remote and remote:
        raise InvalidArgumentError(
            "--remote can't be used without --to-remote"
        )

    if recursive and all_:
        raise InvalidArgumentError("--recursive can't be used with --all")

    stages = set()
    for target in targets:
        stages.update(
            self.stage.collect(
                target,
                recursive=recursive,
                stage_filter=(
                    lambda stage: (stage.is_import or stage.is_repo_import)
                )
                if all_
                else None,
            )
        )

    for stage in stages:
        stage.update(
            rev,
            to_remote=to_remote,
            remote=remote,
            no_download=no_download,
            jobs=jobs,
        )
        dvcfile = Dvcfile(self, stage.path)
        dvcfile.dump(stage)

    return list(stages)
