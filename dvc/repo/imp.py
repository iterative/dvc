from dvc.repo.scm_context import scm_context


@scm_context
def imp(self, url, out, resume=False, fname=None):
    from dvc.stage import Stage

    with self.state:
        stage = Stage.create(
            repo=self, cmd=None, deps=[url], outs=[out], fname=fname
        )

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    with self.state:
        stage.run(resume=resume)

    stage.dump()

    return stage
