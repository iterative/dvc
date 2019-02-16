from __future__ import unicode_literals


def pull(
    self,
    target=None,
    jobs=1,
    remote=None,
    all_branches=False,
    show_checksums=False,
    with_deps=False,
    all_tags=False,
    force=False,
    recursive=False,
):
    processed_files_count = self.fetch(
        target,
        jobs,
        remote=remote,
        all_branches=all_branches,
        all_tags=all_tags,
        show_checksums=show_checksums,
        with_deps=with_deps,
        recursive=recursive,
    )
    self.checkout(
        target=target, with_deps=with_deps, force=force, recursive=recursive
    )
    return processed_files_count
