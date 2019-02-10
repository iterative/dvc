def commit(self, target, with_deps=False, recursive=False, force=False):
    if target and not recursive:
        stages = self.collect(target, with_deps=with_deps)
    else:
        stages = self.active_stages(target)

    with self.state:
        for stage in stages:
            stage.check_can_commit(force=force)
            stage.commit()
            stage.dump()
