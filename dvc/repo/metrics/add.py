from dvc.repo.metrics.modify import modify


def add(repo, path, typ=None, xpath=None):
    if not typ:
        typ = "raw"
    modify(repo, path, typ, xpath)
