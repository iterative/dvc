import os
import re
import typing
from collections.abc import Iterable, Mapping
from functools import singledispatch

from funcy import memoize, rpartial

from dvc.exceptions import DvcException
from dvc.utils.flatten import flatten

if typing.TYPE_CHECKING:
    from typing import List, Match, NoReturn

    from pyparsing import ParseException

    from .context import Context

BRACE_OPEN = "${"
BRACE_CLOSE = "}"
LBRACK = "["
RBRACK = "]"
PERIOD = "."
KEYCRE = re.compile(
    r"""
    (?<!\\)                            # escape \${}
    \${                                # starts with ${
    (?P<inner>.*?)                     # match every char inside
    }                                  # end with {
""",
    re.VERBOSE,
)


@memoize
def get_parser():
    from pyparsing import CharsNotIn, ParserElement, Suppress, ZeroOrMore

    ParserElement.enablePackrat()

    word = CharsNotIn(f"{PERIOD}{LBRACK}{RBRACK}")
    idx = Suppress(LBRACK) + word + Suppress(RBRACK)
    attr = Suppress(PERIOD) + word
    parser = word + ZeroOrMore(attr ^ idx)
    parser.setParseAction(PERIOD.join)

    return parser


class ParseError(DvcException):
    pass


def get_matches(template: str):
    return list(KEYCRE.finditer(template))


def is_interpolated_string(val):
    return isinstance(val, str) and bool(get_matches(val))


def normalize_key(key: str):
    return key.replace(LBRACK, PERIOD).replace(RBRACK, "")


def format_and_raise_parse_error(exc) -> "NoReturn":
    raise ParseError(_format_exc_msg(exc))


def embrace(s: str):
    return BRACE_OPEN + s + BRACE_CLOSE


def escape_str(value):
    if os.name == "nt":
        from subprocess import list2cmdline  # nosec B404

        return list2cmdline([value])
    from shlex import quote

    return quote(value)


@singledispatch
def to_str(obj, config=None) -> str:  # noqa: ARG001, pylint: disable=unused-argument
    return str(obj)


@to_str.register(bool)
def _(obj: bool, config=None):  # noqa: ARG001, pylint: disable=unused-argument
    return "true" if obj else "false"


@to_str.register(dict)
def _(obj: dict, config=None):  # noqa: C901
    config = config or {}

    result = ""
    for k, v in flatten(obj).items():
        if isinstance(v, bool):
            if v:
                result += f"--{k} "
            elif config.get("bool", "store_true") == "boolean_optional":
                result += f"--no-{k} "

        elif isinstance(v, str):
            result += f"--{k} {escape_str(v)} "

        elif isinstance(v, Iterable):
            for n, i in enumerate(v):
                if isinstance(i, str):
                    i = escape_str(i)
                elif isinstance(i, Iterable):
                    raise ParseError(f"Cannot interpolate nested iterable in '{k}'")

                if config.get("list", "nargs") == "append":
                    result += f"--{k} {i} "
                else:
                    result += f"{i} " if n > 0 else f"--{k} {i} "

        else:
            result += f"--{k} {v} "

    return result.rstrip()


def _format_exc_msg(exc: "ParseException"):
    from pyparsing import ParseException

    from dvc.utils import colorize

    exc.loc += 2  # 2 because we append `${` at the start of expr below

    expr = exc.pstr
    exc.pstr = embrace(exc.pstr)
    error = ParseException.explain(exc, depth=0)

    _, pointer, *explains = error.splitlines()
    pstr = "{brace_open}{expr}{brace_close}".format(
        brace_open=colorize(BRACE_OPEN, color="blue"),
        expr=colorize(expr, color="magenta"),
        brace_close=colorize(BRACE_CLOSE, color="blue"),
    )
    msg = "\n".join(explains)
    pointer = colorize(pointer, color="red")
    return "\n".join([pstr, pointer, colorize(msg, color="red", style="bold")])


def recurse(f):
    seq = (list, tuple, set)

    def wrapper(data, *args):
        g = rpartial(wrapper, *args)
        if isinstance(data, Mapping):
            return {g(k): g(v) for k, v in data.items()}
        if isinstance(data, seq):
            return type(data)(map(g, data))
        if isinstance(data, str):
            return f(data, *args)
        return data

    return wrapper


def check_recursive_parse_errors(data):
    func = recurse(check_expression)
    return func(data)


def check_expression(s: str):
    matches = get_matches(s)
    for match in matches:
        get_expression(match)


def parse_expr(s: str):
    from pyparsing import ParseException

    try:
        result = get_parser().parseString(s, parseAll=True)
    except ParseException as exc:
        format_and_raise_parse_error(exc)
        raise AssertionError("unreachable")  # noqa: B904

    joined = result.asList()
    assert len(joined) == 1
    return joined[0]


def get_expression(match: "Match", skip_checks: bool = False):
    inner = match["inner"]
    return inner if skip_checks else parse_expr(inner)


def validate_value(value, key):
    from .context import PRIMITIVES

    not_primitive = value is not None and not isinstance(value, PRIMITIVES)
    not_foreach = key is not None and "foreach" not in key
    if not_primitive and not_foreach:
        if isinstance(value, dict) and key == "cmd":
            return True
        raise ParseError(f"Cannot interpolate data of type '{type(value).__name__}'")


def str_interpolate(
    template: str,
    matches: "List[Match]",
    context: "Context",
    skip_checks: bool = False,
    key=None,
    config=None,
):
    index, buf = 0, ""
    for match in matches:
        start, end = match.span(0)
        expr = get_expression(match, skip_checks=skip_checks)
        value = context.select(expr, unwrap=True)
        validate_value(value, key)
        buf += template[index:start] + to_str(value, config=config)
        index = end
    buf += template[index:]
    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    return buf.replace(r"\${", BRACE_OPEN)


def is_exact_string(src: str, matches: "List[Match]"):
    return len(matches) == 1 and src == matches[0].group(0)
