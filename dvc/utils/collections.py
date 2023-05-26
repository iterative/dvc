from collections.abc import Mapping
from typing import Dict, Iterable, List, Union, no_type_check


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


def nested_contains(dictionary: Dict, phrase: str) -> bool:
    for key, val in dictionary.items():
        if key == phrase and val:
            return True

        if isinstance(val, dict) and nested_contains(val, phrase):
            return True
    return False
