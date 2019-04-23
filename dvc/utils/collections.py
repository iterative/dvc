from __future__ import absolute_import, unicode_literals
from dvc.utils.compat import Mapping


# just simple check for Nones and emtpy strings
def compact(args):
    return list(filter(bool, args))


def apply_diff(src, dest):
    """Recursively apply changes from src to dest.

    Preserves dest type and hidden info in dest structure,
    like ruamel.yaml leaves when parses files. This includes comments,
    ordering and line foldings.

    Used in Stage load/dump cycle to preserve comments and custom formatting.
    """
    Seq = (list, tuple)
    Container = (Mapping, list, tuple)

    def is_same_type(a, b):
        return any(
            isinstance(a, t) and isinstance(b, t)
            for t in [str, Mapping, Seq, bool]
        )

    if isinstance(src, Mapping) and isinstance(dest, Mapping):
        for key, value in src.items():
            if isinstance(value, Container) and is_same_type(
                value, dest.get(key)
            ):
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
                if isinstance(value, Container) and is_same_type(
                    value, dest[i]
                ):
                    apply_diff(value, dest[i])
                elif value != dest[i]:
                    dest[i] = value
    else:
        raise AssertionError(
            "Can't apply diff from {} to {}".format(
                src.__class__.__name__, dest.__class__.__name__
            )
        )
