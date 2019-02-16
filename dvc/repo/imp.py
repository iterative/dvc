def imp(self, url, out, resume=False):
    from dvc.stage import Stage

    stage = Stage.create(repo=self, cmd=None, deps=[url], outs=[out])

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    self.files_to_git_add = []
    with self.state:
        stage.run(resume=resume)

    stage.dump()

    self.remind_to_git_add()

    return stage
