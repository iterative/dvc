import sys
import logging
import colorama
import traceback
import re

from contextlib import contextmanager


def info(message):
    logger.info(message)


def debug(message):
    prefix = colorize('Debug', color='blue')

    out = '{prefix}: {message}'.format(prefix=prefix, message=message)

    logger.debug(out)


def warning(message):
    prefix = colorize('Warning', color='yellow')

    out = '{prefix}: {message}'.format(prefix=prefix, message=message)

    logger.warning(out)


def error(message=None):
    prefix = colorize('Error', color='red')

    out = (
        '{prefix}: {description}'
        '\n'
        '{stack_trace}'
        '\n'
        '{footer}'.format(
            prefix=prefix,
            description=_description(message),
            stack_trace=_stack_trace(),
            footer=_footer(),
        )
    )

    logger.error(out)


def box(message, border_color=None):
    lines = message.split('\n')
    max_width = max(_visual_width(line) for line in lines)

    padding_horizontal = 5
    padding_vertical = 1

    box_size_horizontal = (max_width + (padding_horizontal * 2))

    chars = {
        'corner':     '+',
        'horizontal': '-',
        'vertical':   '|',
        'empty':      ' ',
    }

    margin = "{corner}{line}{corner}\n".format(
        corner=chars['corner'],
        line=chars['horizontal'] * box_size_horizontal,
    )

    padding_lines = [
        "{border}{space}{border}\n".format(
            border=colorize(chars['vertical'], color=border_color),
            space=chars['empty'] * box_size_horizontal,
        ) * padding_vertical
    ]

    content_lines = [
        "{border}{space}{content}{space}{border}\n".format(
            border=colorize(chars['vertical'], color=border_color),
            space=chars['empty'] * padding_horizontal,
            content=_visual_center(line, max_width),
        ) for line in lines
    ]

    box = "{margin}{padding}{content}{padding}{margin}".format(
            margin=colorize(margin, color=border_color),
            padding=''.join(padding_lines),
            content=''.join(content_lines),
        )

    logger.info(box)


def level():
    return logger.getEffectiveLevel()


def set_level(level_name):
    if not level_name:
        return

    levels = {
        'info': logging.INFO,
        'debug': logging.DEBUG,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }

    logger.setLevel(levels.get(level_name))


def be_quiet():
    logger.setLevel(logging.CRITICAL)


def be_verbose():
    logger.setLevel(logging.DEBUG)


@contextmanager
def verbose():
    previous_level = level()
    be_verbose()
    yield
    logger.setLevel(previous_level)


@contextmanager
def quiet():
    previous_level = level()
    be_quiet()
    yield
    logger.setLevel(previous_level)


def is_quiet():
    return level() == logging.CRITICAL


def is_verbose():
    return level() == logging.DEBUG


def colorize(message, color=None):
    if not color:
        return message

    colors = {
        'green': colorama.Fore.GREEN,
        'yellow': colorama.Fore.YELLOW,
        'blue': colorama.Fore.BLUE,
        'red': colorama.Fore.RED,
    }

    return u'{color}{message}{nc}'.format(
        color=colors.get(color, ''),
        message=message,
        nc=colorama.Fore.RESET,
    )


def _init_colorama():
    colorama.init()


def _set_default_level():
    logger.setLevel(logging.INFO)


def _add_handlers():
    formatter = '%(message)s'

    class LogLevelFilter(logging.Filter):
        def filter(self, record):
            return record.levelno <= logging.WARNING

    sh_out = logging.StreamHandler(sys.stdout)
    sh_out.setFormatter(logging.Formatter(formatter))
    sh_out.setLevel(logging.DEBUG)
    sh_out.addFilter(LogLevelFilter())

    sh_err = logging.StreamHandler(sys.stderr)
    sh_err.setFormatter(logging.Formatter(formatter))
    sh_err.setLevel(logging.ERROR)

    logger.addHandler(sh_out)
    logger.addHandler(sh_err)


def _stack_trace():
    trace = sys.exc_info()[2]

    if not is_verbose() or not trace:
        return ''

    return '{line}\n{stack_trace}{line}\n'.format(
        line=colorize('-' * 60, color='red'),
        stack_trace=traceback.format_exc(),
    )


def _description(message):
    exception = sys.exc_info()[1]

    if exception and message:
        description = '{message} - {exception}'
    elif exception:
        description = '{exception}'
    elif message:
        description = '{message}'

    return description.format(message=message, exception=str(exception))


def _footer():
    return '{phrase} Hit us up at {url}, we are always happy to help!'.format(
        phrase=colorize('Having any troubles?', 'yellow'),
        url=colorize('https://dvc.org/support', 'blue'),
    )


def _visual_width(line):
    """Get the the number of columns required to display a string"""

    return len(re.sub(colorama.ansitowin32.AnsiToWin32.ANSI_CSI_RE, '', line))


def _visual_center(line, width):
    """Center align string according to it's visual width"""

    spaces = max(width - _visual_width(line), 0)
    left_padding = int(spaces / 2)
    right_padding = (spaces - left_padding)

    return (left_padding * ' ') + line + (right_padding * ' ')


logger = logging.getLogger('dvc')

_set_default_level()
_add_handlers()
_init_colorama()
