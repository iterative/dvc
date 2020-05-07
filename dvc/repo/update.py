from . import locked
from ..dvcfile import Dvcfile


@locked
def update(self, targets=None, rev=None, recursive=False):
    if not targets:
        stages = self.collect(targets, recursive=recursive)
    else:
        stages = set()
        for target in targets:
            stages.update(self.collect(target, recursive=recursive))

    for stage in stages:
        stage.update(rev)
        dvcfile = Dvcfile(self, stage.path)
        dvcfile.dump(stage)
        stages.add(stage)

    return list(stages)
