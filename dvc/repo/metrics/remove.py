from dvc.repo.metrics.modify import modify


def remove(repo, path):
    modify(repo, path, delete=True)
