import pickle
from typing import Callable, TypeVar

from funcy import decorator

from dvc.exceptions import DvcException

from . import format_link

_R = TypeVar("_R")


@decorator
def with_diskcache(call: Callable[..., _R], name: str) -> _R:
    try:
        return call()
    except (pickle.PickleError, ValueError) as exc:
        if isinstance(exc, ValueError) and not str(exc).startswith(
            "pickle protocol"
        ):
            raise
        link = format_link(
            "https://dvc.org/doc/user-guide/troubleshooting#pickle"
        )
        msg = (
            f"Could not open pickled '{name}' cache. Remove the "
            f"'.dvc/tmp/{name}' directory and then retry this command. "
            f"See {link} for more information."
        )
        raise DvcException(msg) from exc
