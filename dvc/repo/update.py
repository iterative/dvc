def update(self, target):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    with self.state:
        stage.update()
        stage.save()

    stage.dump()
