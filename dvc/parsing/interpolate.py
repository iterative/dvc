import re
import typing
from functools import singledispatch

from pyparsing import (
    CharsNotIn,
    ParseException,
    ParserElement,
    Suppress,
    ZeroOrMore,
)

from dvc.exceptions import DvcException
from dvc.utils import colorize

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
    raise ParseError(_format_exc_msg(exc))


@singledispatch
def to_str(obj):
    return str(obj)


@to_str.register(bool)
def _(obj: bool):
    return "true" if obj else "false"


def _format_exc_msg(exc: ParseException):
    exc.loc += 2  # 2 because we append `${` at the start of expr below

    expr = exc.pstr
    exc.pstr = "${" + exc.pstr + "}"
    error = ParseException.explain(exc, depth=0)

    _, pointer, *explains = error.splitlines()
    pstr = "{brace_open}{expr}{brace_close}".format(
        brace_open=colorize("${", color="blue"),
        expr=colorize(expr, color="magenta"),
        brace_close=colorize("}", color="blue"),
    )
    msg = "\n".join(explains)
    pointer = colorize(pointer, color="red")
    return "\n".join([pstr, pointer, colorize(msg, color="red", style="bold")])


def parse_expr(s: str):
    try:
        result = parser.parseString(s, parseAll=True)
    except ParseException as exc:
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
        buf += template[index:start] + to_str(value)
        index = end
    buf += template[index:]
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return buf.replace(r"\${", "${")


def is_exact_string(src: str, matches: "List[Match]"):
    return len(matches) == 1 and src == matches[0].group(0)
