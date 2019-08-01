from __future__ import unicode_literals


def pull(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    force=False,
    recursive=False,
):
    processed_files_count = self.fetch(
        targets,
        jobs,
        remote=remote,
        all_branches=all_branches,
        all_tags=all_tags,
        with_deps=with_deps,
        recursive=recursive,
    )
    for target in targets or [None]:
        self.checkout(
            target=target,
            with_deps=with_deps,
            force=force,
            recursive=recursive,
        )
    return processed_files_count
