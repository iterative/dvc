from __future__ import unicode_literals


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

        for stage in used["repo"]:
            stage.reproduce()

        return self.cloud.pull(
            used["local"], jobs, remote=remote, show_checksums=show_checksums
        ) + len(used["repo"])
