from . import locked


@locked
def update(self, target, rev=None):
    from ..dvcfile import Dvcfile

    dvcfile = Dvcfile(self, target)
    stage = dvcfile.stage
    stage.update(rev)

    dvcfile.dump(stage)

    return stage
