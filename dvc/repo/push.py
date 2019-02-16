from __future__ import unicode_literals


def push(
    self,
    target=None,
    jobs=1,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
):
    with self.state:
        used = self.used_cache(
            target,
            all_branches=all_branches,
            all_tags=all_tags,
            with_deps=with_deps,
            force=True,
            remote=remote,
            jobs=jobs,
            recursive=recursive,
        )["local"]
        return self.cloud.push(
            used, jobs, remote=remote, show_checksums=show_checksums
        )
