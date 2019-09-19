from __future__ import unicode_literals
from dvc.config import ConfigError


def fetch(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
):
    with self.state:
        used = self.used_cache(
            targets,
            all_branches=all_branches,
            all_tags=all_tags,
            with_deps=with_deps,
            force=True,
            remote=remote,
            jobs=jobs,
            recursive=recursive,
        )

        for dep in used["repo"]:
            dep.fetch()

        try:
            return self.cloud.pull(
                used["local"],
                jobs,
                remote=remote,
                show_checksums=show_checksums,
            ) + len(used["repo"])
        except ConfigError:
            if not used["repo"]:
                raise
