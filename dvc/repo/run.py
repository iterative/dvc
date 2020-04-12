from . import locked
from .scm_context import scm_context


@locked
@scm_context
def run(self, no_exec=False, **kwargs):
    from dvc.stage import Stage
    from dvc.dvcfile import Dvcfile

    stage = Stage.create(self, **kwargs)
    if stage is None:
        return None

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.overwrite_with_prompt(force=kwargs.get("overwrite", True))

    self.check_modified_graph([stage])

    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))

    dvcfile.dump(stage)

    return stage
