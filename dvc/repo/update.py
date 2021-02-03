from . import locked


@locked
def update(
    self,
    targets=None,
    rev=None,
    recursive=False,
    to_remote=False,
    remote=None,
    jobs=None,
):
    from ..dvcfile import Dvcfile

    if not targets:
        targets = [None]

    if isinstance(targets, str):
        targets = [targets]

    stages = set()
    for target in targets:
        stages.update(self.stage.collect(target, recursive=recursive))

    for stage in stages:
        stage.update(rev, to_remote=to_remote, remote=remote, jobs=jobs)
        dvcfile = Dvcfile(self, stage.path)
        dvcfile.dump(stage)
        stages.add(stage)

    return list(stages)
