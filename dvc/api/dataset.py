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


def _get_raw(name: str) -> Dict[str, Any]:
    datasets = _get_datasets()
    return datasets[name]


def get(cls: "Type[T]", name: str) -> "T":
    from pydantic import TypeAdapter

    d = _get_raw(name)
    t = TypeAdapter(cls)
    return t.validate_python(d)
