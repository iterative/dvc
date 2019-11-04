from . import locked as locked_repo
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output


@locked_repo
@scm_context
def imp_url(self, url, out=None, fname=None, erepo=None, locked=True):
    from dvc.stage import Stage

    out = resolve_output(url, out)

    stage = Stage.create(
        self, cmd=None, deps=[url], outs=[out], fname=fname, erepo=erepo
    )

    if stage is None:
        return None

    self.check_modified_graph([stage])

    stage.run()

    stage.locked = locked

    stage.dump()

    return stage
