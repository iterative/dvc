from . import locked


@locked
def update(self, target, rev=None):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    stage.update(rev)

    stage.dump()

    return stage
