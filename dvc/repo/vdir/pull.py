from pathlib import Path

from .. import locked


@locked
def pull(
    repo,
    targets=None,
    jobs=None,
    remote=None,
    all_branches=False,
    with_deps=False,
    all_tags=False,
    recursive=False,
    all_commits=False,
):
    used = repo.used_cache(
        targets,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        with_deps=with_deps,
        force=True,
        remote=remote,
        jobs=jobs,
        recursive=recursive,
    )

    dirs = _create_dirs(used)

    return dirs


def _create_dirs(used):
    dirs = set()
    for _, files in used.scheme_names("local"):
        for f in files:
            d = Path(f).parent
            dirs.add(d)
            d.mkdir(parents=True, exist_ok=True)
    return dirs
