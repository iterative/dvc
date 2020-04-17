from . import locked
from dvc.dvcfile import Dvcfile


@locked
def commit(self, target, with_deps=False, recursive=False, force=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        stage.check_can_commit(force=force)
        stage.commit()

        Dvcfile(self, stage.path).dump(stage)
