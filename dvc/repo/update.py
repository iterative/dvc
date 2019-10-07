from . import locked


@locked
def update(self, target):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    stage.update()

    stage.dump()
