from dvc.exceptions import UpdateWithRevNotPossibleError

from . import locked


@locked
def update(self, target, rev=None):
    from dvc.stage import Stage

    stage = Stage.load(self, target)

    if not stage.is_repo_import and rev:
        raise UpdateWithRevNotPossibleError()

    stage.update(rev=rev)

    stage.dump()
