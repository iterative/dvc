"""Manages logger for dvc repo."""

from __future__ import unicode_literals

from dvc.exceptions import DvcException
from dvc.utils.compat import str
from dvc.progress import progress_aware

import re
import sys
import logging
import traceback

from contextlib import contextmanager

import colorama


@progress_aware
def info(message):
    """Prints an info message."""
    logger.info(message)


# Do not annotate debug with progress_aware. Why? Debug messages are called
# in analytics.py. Trigger introduced by @progress_aware may run even
# if log_level != DEBUG (look at Progress.print) Which might result in
# messy terminal after any command (because analytics run in separate thread)
def debug(message):
    """Prints a debug message."""
    prefix = colorize("Debug", color="blue")

    out = "{prefix}: {message}".format(prefix=prefix, message=message)

    logger.debug(out)


@progress_aware
def warning(message, parse_exception=False):
    """Prints a warning message."""
    prefix = colorize("Warning", color="yellow")

    exception, stack_trace = None, ""
    if parse_exception:
        exception, stack_trace = _parse_exc()

    out = "{prefix}: {description}".format(
        prefix=prefix, description=_description(message, exception)
    )

    if stack_trace:
        out += "\n{stack_trace}".format(stack_trace=stack_trace)

    logger.warning(out)


@progress_aware
def error(message=None):
    """Prints an error message."""
    prefix = colorize("Error", color="red")

    exception, stack_trace = _parse_exc()

    out = (
        "{prefix}: {description}"
        "\n"
        "{stack_trace}"
        "\n"
        "{footer}".format(
            prefix=prefix,
            description=_description(message, exception),
            stack_trace=stack_trace,
            footer=_footer(),
        )
    )

    logger.error(out)


def box(message, border_color=None):
    """Prints a message in a box.

    Args:
        message (unicode): message to print.
        border_color (unicode): name of a color to outline the box with.
    """
    lines = message.split("\n")
    max_width = max(_visual_width(line) for line in lines)

    padding_horizontal = 5
    padding_vertical = 1

    box_size_horizontal = max_width + (padding_horizontal * 2)

    chars = {"corner": "+", "horizontal": "-", "vertical": "|", "empty": " "}

    margin = "{corner}{line}{corner}\n".format(
        corner=chars["corner"], line=chars["horizontal"] * box_size_horizontal
    )

    padding_lines = [
        "{border}{space}{border}\n".format(
            border=colorize(chars["vertical"], color=border_color),
            space=chars["empty"] * box_size_horizontal,
        )
        * padding_vertical
    ]

    content_lines = [
        "{border}{space}{content}{space}{border}\n".format(
            border=colorize(chars["vertical"], color=border_color),
            space=chars["empty"] * padding_horizontal,
            content=_visual_center(line, max_width),
        )
        for line in lines
    ]

    box_str = "{margin}{padding}{content}{padding}{margin}".format(
        margin=colorize(margin, color=border_color),
        padding="".join(padding_lines),
        content="".join(content_lines),
    )

    logger.info(box_str)


def level():
    """Returns current log level."""
    return logger.getEffectiveLevel()


def set_level(level_name):
    """Sets log level.

    Args:
        level_name (str): log level name. E.g. info, debug, warning, error,
            critical.
    """
    if not level_name:
        return

    levels = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    logger.setLevel(levels.get(level_name))


def be_quiet():
    """Disables all messages except critical ones."""
    logger.setLevel(logging.CRITICAL)


def be_verbose():
    """Enables all messages."""
    logger.setLevel(logging.DEBUG)


@contextmanager
def verbose():
    """Enables verbose mode for the context."""
    previous_level = level()
    be_verbose()
    yield
    logger.setLevel(previous_level)


@contextmanager
def quiet():
    """Enables quiet mode for the context."""
    previous_level = level()
    be_quiet()
    yield
    logger.setLevel(previous_level)


def is_quiet():
    """Returns whether or not all messages except critical ones are
    disabled.
    """
    return level() == logging.CRITICAL


def is_verbose():
    """Returns whether or not all messages are enabled."""
    return level() == logging.DEBUG


def colorize(message, color=None):
    """Returns a message in a specified color."""
    if not color:
        return message

    colors = {
        "green": colorama.Fore.GREEN,
        "yellow": colorama.Fore.YELLOW,
        "blue": colorama.Fore.BLUE,
        "red": colorama.Fore.RED,
    }

    return "{color}{message}{nc}".format(
        color=colors.get(color, ""), message=message, nc=colorama.Fore.RESET
    )


def set_default_level():
    """Sets default log level."""
    logger.setLevel(logging.INFO)


def _add_handlers():
    # NOTE: We need to initialize colorama before setting the stream handlers
    #       so it can wrap stdout/stderr and convert color codes to Windows.
    colorama.init()

    formatter = "%(message)s"

    class _LogLevelFilter(logging.Filter):
        # pylint: disable=too-few-public-methods
        def filter(self, record):
            return record.levelno <= logging.WARNING

    sh_out = logging.StreamHandler(sys.stdout)
    sh_out.setFormatter(logging.Formatter(formatter))
    sh_out.setLevel(logging.DEBUG)
    sh_out.addFilter(_LogLevelFilter())

    sh_err = logging.StreamHandler(sys.stderr)
    sh_err.setFormatter(logging.Formatter(formatter))
    sh_err.setLevel(logging.ERROR)

    logger.addHandler(sh_out)
    logger.addHandler(sh_err)


def _walk_exc(exc):
    exc_list = [str(exc)]
    tb_list = [traceback.format_exc()]

    # NOTE: parsing chained exceptions. See dvc/exceptions.py for more info.
    while hasattr(exc, "cause") and exc.cause is not None:
        exc_list.append(str(exc.cause))
        if hasattr(exc, "cause_tb") and exc.cause_tb is not None:
            tb_list.insert(0, str(exc.cause_tb))
        exc = exc.cause

    return exc_list, tb_list


def _parse_exc():
    exc = sys.exc_info()[1]
    if not exc:
        return (None, "")

    exc_list, tb_list = _walk_exc(exc)

    exception = ": ".join(exc_list)

    if is_verbose():
        stack_trace = "{line}\n{stack_trace}{line}\n".format(
            line=colorize("-" * 60, color="red"),
            stack_trace="\n".join(tb_list),
        )
    else:
        stack_trace = ""

    return (exception, stack_trace)


def _description(message, exception):
    if exception and message:
        description = "{message} - {exception}"
    elif exception:
        description = "{exception}"
    elif message:
        description = "{message}"
    else:
        raise DvcException(
            "Unexpected error - either exception or message must be provided"
        )

    return description.format(message=message, exception=exception)


def _footer():
    return "{phrase} Hit us up at {url}, we are always happy to help!".format(
        phrase=colorize("Having any troubles?", "yellow"),
        url=colorize("https://dvc.org/support", "blue"),
    )


def _visual_width(line):
    """Get the the number of columns required to display a string"""

    return len(re.sub(colorama.ansitowin32.AnsiToWin32.ANSI_CSI_RE, "", line))


def _visual_center(line, width):
    """Center align string according to it's visual width"""

    spaces = max(width - _visual_width(line), 0)
    left_padding = int(spaces / 2)
    right_padding = spaces - left_padding

    return (left_padding * " ") + line + (right_padding * " ")


logger = logging.getLogger("dvc")  # pylint: disable=invalid-name

set_default_level()
_add_handlers()
