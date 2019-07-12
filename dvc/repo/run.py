from __future__ import unicode_literals

from dvc.repo.scm_context import scm_context


@scm_context
def run(self, no_exec=False, **kwargs):
    from dvc.stage import Stage

    with self.state:
        stage = Stage.create(self, **kwargs)

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    with self.state:
        if not no_exec:
            stage.run(no_commit=kwargs.get("no_commit", False))

    stage.dump()

    return stage
