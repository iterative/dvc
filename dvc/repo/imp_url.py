from . import locked as locked_repo
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output


@locked_repo
@scm_context
def imp_url(self, url, out=None, fname=None, erepo=None, locked=True):
    from dvc.dvcfile import Dvcfile

    out = resolve_output(url, out)
    stage = Dvcfile.create_stage(
        self,
        cmd=None,
        deps=[url],
        outs=[out],
        erepo=erepo,
        accompany_outs=True,
        fname=fname,
    )

    if stage is None:
        return None

    self.check_modified_graph([stage])

    stage.run()

    stage.locked = locked

    Dvcfile(self, stage.path).dump(stage)

    return stage
