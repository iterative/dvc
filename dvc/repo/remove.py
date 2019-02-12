def remove(self, target, outs_only=False):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    if outs_only:
        stage.remove_outs()
    else:
        stage.remove()

    return stage
