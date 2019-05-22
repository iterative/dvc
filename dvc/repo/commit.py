def commit(self, target, with_deps=False, recursive=False, force=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    with self.state:
        for stage in stages:
            stage.check_can_commit(force=force)
            stage.commit()
            stage.dump()
