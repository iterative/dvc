import re
import typing
from collections.abc import Mapping

from funcy import rpartial

if typing.TYPE_CHECKING:
    from .context import Context

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

UNWRAP_DEFAULT = True


def get_matches(template: str):
    return list(KEYCRE.finditer(template))


def is_interpolated_string(val):
    return bool(get_matches(val)) if isinstance(val, str) else False


def _unwrap(value):
    from .context import Value

    if isinstance(value, Value):
        return value.value
    return value


def _resolve_value(match, context: "Context", unwrap=UNWRAP_DEFAULT):
    _, _, inner = match.groups()
    value = context.select(inner)
    return _unwrap(value) if unwrap else value


def _str_interpolate(template, matches, context):
    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        buf += template[index:start] + str(_resolve_value(match, context))
        index = end
    return buf + template[index:]


def is_exact_string(src: str, matches):
    return len(matches) == 1 and src == matches[0].group(0)


def resolve_str(src: str, context, unwrap=UNWRAP_DEFAULT):
    matches = get_matches(src)
    if is_exact_string(src, matches):
        # replace "${enabled}", if `enabled` is a boolean, with it's actual
        # value rather than it's string counterparts.
        return _resolve_value(matches[0], context, unwrap=unwrap)

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
        return resolve_str(src, context)
    return src
