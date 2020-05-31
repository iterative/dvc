from . import locked


@locked
def _set(repo, target, frozen):
    from dvc.utils import parse_target

    path, name = parse_target(target)
    stage = repo.get_stage(path, name)
    stage.frozen = frozen
    stage.dvcfile.dump(stage, update_pipeline=True)

    return stage


def freeze(repo, target):
    return _set(repo, target, True)


def unfreeze(repo, target):
    return _set(repo, target, False)
