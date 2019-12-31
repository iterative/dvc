from . import locked
from .scm_context import scm_context


@locked
@scm_context
def run(self, no_exec=False, **kwargs):
    from dvc.stage import Stage

    stage = Stage.create(self, **kwargs)

    if stage is None:
        return None

    self.check_modified_graph([stage])

    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))

    stage.dump()

    return stage
