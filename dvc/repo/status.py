from __future__ import unicode_literals

import logging


logger = logging.getLogger(__name__)


def _local_status(self, target=None, with_deps=False):
    status = {}

    stages = self.collect(target, with_deps=with_deps)

    for stage in stages:
        if stage.locked:
            logger.warning(
                "DVC file '{path}' is locked. Its dependencies are"
                " not going to be shown in the status output.".format(
                    path=stage.relpath
                )
            )

        status.update(stage.status())

    return status


def _cloud_status(
    self,
    target=None,
    jobs=1,
    remote=None,
    show_checksums=False,
    all_branches=False,
    with_deps=False,
    all_tags=False,
):
    import dvc.remote.base as cloud

    used = self.used_cache(
        target,
        all_branches=all_branches,
        all_tags=all_tags,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
    )["local"]

    ret = {}
    status_info = self.cloud.status(
        used, jobs, remote=remote, show_checksums=show_checksums
    )
    for md5, info in status_info.items():
        name = info["name"]
        status = info["status"]
        if status in [cloud.STATUS_OK, cloud.STATUS_MISSING]:
            continue

        prefix_map = {cloud.STATUS_DELETED: "deleted", cloud.STATUS_NEW: "new"}

        ret[name] = prefix_map[status]

    return ret


def status(
    self,
    target=None,
    jobs=1,
    cloud=False,
    remote=None,
    show_checksums=False,
    all_branches=False,
    with_deps=False,
    all_tags=False,
):
    with self.state:
        if cloud or remote:
            return _cloud_status(
                self,
                target,
                jobs,
                remote=remote,
                show_checksums=show_checksums,
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
            )
        return _local_status(self, target, with_deps=with_deps)
