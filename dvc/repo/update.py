from . import locked


@locked
def update(self, target):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    with self.state:
        stage.update()

    stage.dump()
