import logging
from copy import copy

logger = logging.getLogger(__name__)


def add(self, tag, target=None, with_deps=False, recursive=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        changed = False
        for out in stage.outs:
            if not out.info:
                logger.warning("missing checksum info for '{}'".format(out))
                continue
            out.tags[tag] = copy(out.info)
            changed = True
        if changed:
            stage.dump()
