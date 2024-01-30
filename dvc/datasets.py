from typing import Any


def get(name: str) -> dict[str, dict[str, Any]]:
    from dvc.repo import Repo

    with Repo() as repo:
        datasets = repo.index.datasets
        return datasets[name]
