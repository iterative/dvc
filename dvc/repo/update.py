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
):
    from ..dvcfile import Dvcfile

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

    stages = set()
    for target in targets:
        stages.update(self.stage.collect(target, recursive=recursive))

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
