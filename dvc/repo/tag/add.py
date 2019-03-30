import dvc.logger as logger


def add(self, tag, target=None, with_deps=False, recursive=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        changed = False
        for out in stage.outs:
            if not out.info:
                logger.warning("missing checksum info for '{}'".format(out))
                continue
            out.tags[tag] = out.info.copy()
            changed = True
        if changed:
            stage.dump()
