import re
import typing

if typing.TYPE_CHECKING:
    from typing import List, Match

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


def get_matches(template: str):
    return list(KEYCRE.finditer(template))


def is_interpolated_string(val):
    return bool(get_matches(val)) if isinstance(val, str) else False


def get_expression(match: "Match"):
    _, _, inner = match.groups()
    return inner


def str_interpolate(template: str, matches: "List[Match]", context: "Context"):
    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        expr = get_expression(match)
        buf += template[index:start] + str(context.select(expr))
        index = end
    buf += template[index:]
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return buf.replace(r"\${", "${")


def is_exact_string(src: str, matches: "List[Match]"):
    return len(matches) == 1 and src == matches[0].group(0)
