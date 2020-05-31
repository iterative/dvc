def _revisions(revs, is_dirty):
    revisions = revs or []
    if len(revisions) <= 1:
        if len(revisions) == 0 and is_dirty:
            revisions.append("HEAD")
        revisions.append("workspace")
    return revisions


def diff(repo, *args, revs=None, **kwargs):
    return repo.plots.show(
        *args, revs=_revisions(revs, repo.scm.is_dirty()), **kwargs
    )
