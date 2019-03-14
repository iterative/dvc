def list(self, target=None, with_deps=False, recursive=False):
    ret = {}
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        outs_tags = {}
        for out in stage.outs:
            if out.tags:
                outs_tags[str(out)] = out.tags.copy()
        if outs_tags:
            ret.update({stage.relpath: outs_tags})
    return ret
