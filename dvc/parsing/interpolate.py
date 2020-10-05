import re
from collections.abc import Mapping

from funcy import lmap, rpartial

from dvc.parsing.context import Value

KEYCRE = re.compile(
    r"""
    (?<!\\)                   # escape \${} or ${{}}
    \$                        # starts with $
    (?:({{)|({))              # either starts with double braces or single
    ([\w._ \\/-]*?)           # match every char, attr access through "."
    (?(1)}})(?(2)})           # end with same kinds of braces it opened with
""",
    re.VERBOSE,
)


def _find_match(template):
    return list(KEYCRE.finditer(template))


def _resolve_value(match, context):
    expand_and_track, _, inner = match.groups()
    return context.select(inner, track=expand_and_track)


def _str_interpolate(template, matches, context):
    ret = ""
    idx = 0
    for match in matches:
        value = _resolve_value(match, context)
        ret += template[idx : match.start(0)]
        ret += str(value)
        idx = match.end()
    return ret + template[idx:]


def _resolve_str(src, context):
    if isinstance(src, str):
        matches = _find_match(src)
        num_matches = len(matches)
        if num_matches:
            # replace "${enabled}", if `enabled` is a boolean, with it's actual
            # value rather than it's string counterparts.
            if num_matches == 1 and src == matches[0].group(0):
                value = _resolve_value(matches[0], context)
                return value.value
            # but not "${num} days"
            src = _str_interpolate(src, matches, context)

    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return src.replace(r"\${", "${")


def resolve(src, context):
    # TODO: can we do this better?
    Seq = (list, tuple, set)

    apply_value = rpartial(resolve, context)
    if isinstance(src, Mapping):
        return {key: apply_value(value) for key, value in src.items()}
    elif isinstance(src, Seq):
        return lmap(apply_value, src)
    return _resolve_str(src, context)
