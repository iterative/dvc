import os

from dvc.utils.compat import urlparse
from dvc.repo.scm_context import scm_context


@scm_context
def imp_url(
    self, url, out=None, resume=False, fname=None, erepo=None, locked=False
):
    from dvc.stage import Stage

    default_out = os.path.basename(urlparse(url).path)
    out = out or default_out

    with self.state:
        stage = Stage.create(
            repo=self,
            cmd=None,
            deps=[url],
            outs=[out],
            fname=fname,
            erepo=erepo,
        )

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    with self.state:
        stage.run(resume=resume)

    stage.locked = locked

    stage.dump()

    return stage
