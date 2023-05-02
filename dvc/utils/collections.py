import inspect
from collections.abc import Mapping
from functools import wraps
from typing import Callable, Dict, Iterable, List, TypeVar, Union, no_type_check


@no_type_check
def apply_diff(src, dest):  # noqa: C901
    """Recursively apply changes from src to dest.

    Preserves dest type and hidden info in dest structure,
    like ruamel.yaml leaves when parses files. This includes comments,
    ordering and line foldings.

    Used in Stage load/dump cycle to preserve comments and custom formatting.
    """
    Seq = (list, tuple)  # noqa: N806
    Container = (Mapping, list, tuple)  # noqa: N806

    def is_same_type(a, b):
        return any(
            isinstance(a, t) and isinstance(b, t) for t in [str, Mapping, Seq, bool]
        )

    if isinstance(src, Mapping) and isinstance(dest, Mapping):
        for key, value in src.items():
            if isinstance(value, Container) and is_same_type(value, dest.get(key)):
                apply_diff(value, dest[key])
            elif key not in dest or value != dest[key]:
                dest[key] = value
        for key in set(dest) - set(src):
            del dest[key]
    elif isinstance(src, Seq) and isinstance(dest, Seq):
        if len(src) != len(dest):
            dest[:] = src
        else:
            for i, value in enumerate(src):
                if isinstance(value, Container) and is_same_type(value, dest[i]):
                    apply_diff(value, dest[i])
                elif value != dest[i]:
                    dest[i] = value
    else:
        raise AssertionError(  # noqa: TRY004
            "Can't apply diff from {} to {}".format(
                src.__class__.__name__, dest.__class__.__name__
            )
        )


def to_omegaconf(item):
    """
    Some parsers return custom classes (i.e. parse_yaml_for_update)
    that can mess up with omegaconf logic.
    Cast the custom classes to Python primitives.
    """
    if isinstance(item, dict):
        return {k: to_omegaconf(v) for k, v in item.items()}
    if isinstance(item, list):
        return [to_omegaconf(x) for x in item]
    return item


def remove_missing_keys(src, to_update):
    keys = list(src.keys())
    for key in keys:
        if key not in to_update:
            del src[key]
        elif isinstance(src[key], dict):
            remove_missing_keys(src[key], to_update[key])

    return src


def _merge_item(d, key, value):
    if key in d:
        item = d.get(key, None)
        if isinstance(item, dict) and isinstance(value, dict):
            merge_dicts(item, value)
        else:
            d[key] = value
    else:
        d[key] = value


def merge_dicts(src: Dict, to_update: Dict) -> Dict:
    """Recursively merges dictionaries.

    Args:
        src (dict): source dictionary of parameters
        to_update (dict): dictionary of parameters to merge into src
    """
    for key, value in to_update.items():
        _merge_item(src, key, value)
    return src


def ensure_list(item: Union[Iterable[str], str, None]) -> List[str]:
    if item is None:
        return []
    if isinstance(item, str):
        return [item]
    return list(item)


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


def chunk_dict(d: Dict[_KT, _VT], size: int = 1) -> List[Dict[_KT, _VT]]:
    from funcy import chunks

    return [{key: d[key] for key in chunk} for chunk in chunks(size, d)]


class _NamespacedDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def validate(*validators: Callable, post: bool = False):
    """
    Validate and transform arguments and results from function calls.

    The validators functions are passed a dictionary of arguments, which
    supports dot notation access too.

    The key is derived from the function signature, and hence is the name of
    the argument, whereas the value is the one passed to the function
    (if it is not passed, default value from keyword arguments are provided).

    >>> def validator(args):
    ...    assert args["l"] >= 0 and args.b >= 0 and args.h >= 0

    >>> @validate(validator)
    ... def cuboid_area(l, b, h=1):
    ...   return 2*(l*b + l*h + b*h)

    >>> cuboid_area(5, 20)
    250
    >>> cuboid_area(-1, -2)
    Traceback (most recent call last):
      ...
    AssertionError
    """

    def wrapped(func: Callable):
        sig = inspect.signature(func)

        @wraps(func)
        def inner(*args, **kwargs):
            ba = sig.bind(*args, **kwargs)
            ba.apply_defaults()
            ba.arguments = _NamespacedDict(ba.arguments)  # type: ignore[assignment]

            if not post:
                for validator in validators:
                    validator(ba.arguments)

            result = func(*ba.args, **ba.kwargs)
            if post:
                for validator in validators:
                    result = validator(result)
            return result

        return inner

    return wrapped


def nested_contains(dictionary: Dict, phrase: str) -> bool:
    for key, val in dictionary.items():
        if key == phrase and val:
            return True

        if isinstance(val, dict) and nested_contains(val, phrase):
            return True
    return False
