from . import locked


@locked
def lock(self, target, unlock=False):
    from ..dvcfile import Dvcfile

    dvcfile = Dvcfile(self, target)
    stage = dvcfile.load()
    stage.locked = False if unlock else True
    dvcfile.dump(stage)

    return stage
