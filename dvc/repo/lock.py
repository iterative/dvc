def lock(self, target, unlock=False):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    stage.locked = False if unlock else True
    stage.dump()

    return stage
