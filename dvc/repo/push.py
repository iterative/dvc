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
    revs=None,
    glob=False,
):
    used_run_cache = self.stage_cache.push(remote) if run_cache else []

    if isinstance(targets, str):
        targets = [targets]

    if glob:
        from glob import iglob

        expanded_targets = [
            exp_target
            for target in targets
            for exp_target in iglob(target, recursive=True)
        ]
    else:
        expanded_targets = targets

    used = self.used_cache(
        expanded_targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
        used_run_cache=used_run_cache,
        revs=revs,
    )

    return len(used_run_cache) + self.cloud.push(used, jobs, remote=remote)
