from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar

from funcy import memoize

if TYPE_CHECKING:
    from pydantic import BaseModel

    T = TypeVar("T", bound=BaseModel)


from dvc.datasets import DVCDataset, DVCxDataset, WebDataset

__all__ = ["DVCDataset", "DVCxDataset", "WebDataset", "get"]


@memoize
def _get_datasets() -> Dict[str, Dict[str, Any]]:
    from dvc.repo import Repo

    with Repo() as repo:
        return repo.index.datasets


def get(cls: "Type[T]", name: str) -> "T":
    datasets = _get_datasets()
    return cls.model_validate(datasets[name])
