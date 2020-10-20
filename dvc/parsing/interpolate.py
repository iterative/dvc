import re
from collections.abc import Mapping

from funcy import rpartial

from dvc.parsing.context import Context, Value

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


def _get_matches(template):
    return list(KEYCRE.finditer(template))


def _unwrap(value):
    if isinstance(value, Value):
        return value.value
    return value


def _resolve_value(match, context: Context):
    _, _, inner = match.groups()
    value = context.select(inner)
    return _unwrap(value)


def _str_interpolate(template, matches, context):
    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        buf += template[index:start] + str(_resolve_value(match, context))
        index = end
    return buf + template[index:]


def _resolve_str(src: str, context):
    matches = _get_matches(src)
    if len(matches) == 1 and src == matches[0].group(0):
        # replace "${enabled}", if `enabled` is a boolean, with it's actual
        # value rather than it's string counterparts.
        return _resolve_value(matches[0], context)

    # but not "${num} days"
    src = _str_interpolate(src, matches, context)
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return src.replace(r"\${", "${")


def resolve(src, context):
    Seq = (list, tuple, set)

    apply_value = rpartial(resolve, context)
    if isinstance(src, Mapping):
        return {key: apply_value(value) for key, value in src.items()}
    elif isinstance(src, Seq):
        return type(src)(map(apply_value, src))
    elif isinstance(src, str):
        return _resolve_str(src, context)
    return src
