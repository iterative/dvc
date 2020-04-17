from . import locked


@locked
def remove(self, target, outs_only=False):
    from ..dvcfile import Dvcfile

    stage = Dvcfile(self, target).stage
    if outs_only:
        stage.remove_outs(force=True)
    else:
        stage.remove(force=True)

    return stage
