def remove(self, target, outs_only=False):
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    if outs_only:
        stage.remove_outs(force=True)
    else:
        stage.remove(force=True)

    return stage
