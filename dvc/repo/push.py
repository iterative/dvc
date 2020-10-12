from . import locked


@locked
def push(
    self,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
    run_cache=False,
):
    used_run_cache = self.stage_cache.push(remote) if run_cache else []

    if isinstance(targets, str):
        targets = [targets]

    used = self.used_cache(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        used_run_cache=used_run_cache,
    )

    return len(used_run_cache) + self.cloud.push(used, jobs, remote=remote)
