import re
import typing

from pyparsing import (
    CharsNotIn,
    ParseException,
    ParserElement,
    Suppress,
    ZeroOrMore,
)

from dvc.exceptions import DvcException

if typing.TYPE_CHECKING:
    from typing import List, Match

    from .context import Context

LBRACK = "["
RBRACK = "]"
PERIOD = "."
KEYCRE = re.compile(
    r"""
    (?<!\\)                   # escape \${} or ${{}}
    \$                        # starts with $
    (?:({{)|({))              # either starts with double braces or single
    (.*?)                     # match every char inside
    (?(1)}})(?(2)})           # end with same kinds of braces it opened with
""",
    re.VERBOSE,
)

ParserElement.enablePackrat()


word = CharsNotIn(f"{PERIOD}{LBRACK}{RBRACK}")
idx = Suppress(LBRACK) + word + Suppress(RBRACK)
attr = Suppress(PERIOD) + word
parser = word + ZeroOrMore(attr ^ idx)
parser.setParseAction(PERIOD.join)


class ParseError(DvcException):
    pass


def get_matches(template: str):
    return list(KEYCRE.finditer(template))


def is_interpolated_string(val):
    return bool(get_matches(val)) if isinstance(val, str) else False


def format_and_raise_parse_error(exc):
    msg = ParseException.explain(exc, depth=0)
    raise ParseError(msg)


def parse_expr(s: str):
    try:
        result = parser.parseString(s, parseAll=True)
    except ParseException as exc:
        exc.pstr = "${" + exc.pstr + "}"
        exc.loc += 2
        format_and_raise_parse_error(exc)

    joined = result.asList()
    assert len(joined) == 1
    return joined[0]


def get_expression(match: "Match"):
    _, _, inner = match.groups()
    return parse_expr(inner)


def str_interpolate(template: str, matches: "List[Match]", context: "Context"):
    from .context import PRIMITIVES

    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        expr = get_expression(match)
        value = context.select(expr, unwrap=True)
        if value is not None and not isinstance(value, PRIMITIVES):
            raise ParseError(
                f"Cannot interpolate data of type '{type(value).__name__}'"
            )
        buf += template[index:start] + str(value)
        index = end
    buf += template[index:]
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return buf.replace(r"\${", "${")


def is_exact_string(src: str, matches: "List[Match]"):
    return len(matches) == 1 and src == matches[0].group(0)
