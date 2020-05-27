from . import locked


@locked
def lock(self, target, unlock=False):
    from dvc.utils import parse_target

    path, name = parse_target(target)
    stage = self.get_stage(path, name)
    stage.locked = False if unlock else True
    stage.dvcfile.dump(stage, update_pipeline=True)

    return stage
