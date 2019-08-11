from dvc.utils.compat import pathlib
from dvc.repo.scm_context import scm_context


@scm_context
def imp_url(self, url, out=None, fname=None, erepo=None, locked=True):
    from dvc.stage import Stage

    out = out or pathlib.PurePath(url).name

    with self.state:
        stage = Stage.create(
            self, cmd=None, deps=[url], outs=[out], fname=fname, erepo=erepo
        )

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    with self.state:
        stage.run()

    stage.locked = locked

    stage.dump()

    return stage
