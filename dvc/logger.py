import sys
import logging
import colorama
import traceback
import re


colorama.init()


def visual_width(line):
    """ Get the the number of columns required to display a string """

    return len(re.sub(colorama.ansitowin32.AnsiToWin32.ANSI_CSI_RE, '', line))


def visual_center(line, width):
    """ Center align string according to it's visual width """

    spaces = max(width - visual_width(line), 0)
    left_padding = int(spaces / 2)
    right_padding = (spaces - left_padding)

    return (left_padding * ' ') + line + (right_padding * ' ')


class Logger(object):
    FMT = '%(message)s'
    DEFAULT_LEVEL = logging.INFO
    DEFAULT_LEVEL_NAME = 'info'

    LEVEL_MAP = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARNING,
        'error': logging.ERROR
    }

    COLOR_MAP = {
        'green': colorama.Fore.GREEN,
        'yellow': colorama.Fore.YELLOW,
        'blue': colorama.Fore.BLUE,
        'red': colorama.Fore.RED,
    }

    LEVEL_COLOR_MAP = {
        'debug': 'blue',
        'warn': 'yellow',
        'error': 'red',
    }

    def __init__(self, loglevel=None, force=False):
        self.logger = logging.getLogger('dvc')
        if force or not self._already_initialized():
            class LogLevelFilter(logging.Filter):
                def filter(self, record):
                    return record.levelno <= logging.WARNING

            sh_out = logging.StreamHandler(sys.stdout)
            sh_out.setFormatter(logging.Formatter(self.FMT))
            sh_out.setLevel(logging.DEBUG)
            sh_out.addFilter(LogLevelFilter())

            sh_err = logging.StreamHandler(sys.stderr)
            sh_err.setFormatter(logging.Formatter(self.FMT))
            sh_err.setLevel(logging.ERROR)

            self.logger.addHandler(sh_out)
            self.logger.addHandler(sh_err)

        self.set_level(loglevel)

    def set_level(self, level=None):
        if not level:
            lvl = self.DEFAULT_LEVEL
        else:
            level = level.lower()
            lvl = self.LEVEL_MAP.get(level, self.DEFAULT_LEVEL)
        self.logger.setLevel(lvl)
        self.lvl = lvl

    def be_quiet(self):
        self.lvl = logging.CRITICAL
        self.logger.setLevel(logging.CRITICAL)

    def be_verbose(self):
        self.lvl = logging.DEBUG
        self.logger.setLevel(logging.DEBUG)

    def colorize(self, msg, color):
        header = ''
        footer = ''

        if sys.stdout.isatty():  # pragma: no cover
            header = self.COLOR_MAP.get(color.lower(), '')
            footer = colorama.Style.RESET_ALL

        return u'{}{}{}'.format(header, msg, footer)

    def parse_exc(self, exc, tb=None):
        str_tb = tb if tb else None
        str_exc = str(exc) if exc else ""
        l_str_exc = []

        if len(str_exc) != 0:
            l_str_exc.append(str_exc)

        if exc and hasattr(exc, 'cause') and exc.cause:
            cause_tb = exc.cause_tb if hasattr(exc, 'cause_tb') else None
            l_cause_str_exc, cause_str_tb = self.parse_exc(exc.cause, cause_tb)

            str_tb = cause_str_tb
            l_str_exc += l_cause_str_exc

        return (l_str_exc, str_tb)

    def _prefix(self, msg, typ):
        color = self.LEVEL_COLOR_MAP.get(typ.lower(), '')
        return self.colorize('{}'.format(msg), color)

    def error_prefix(self):
        return self._prefix('Error', 'error')

    def warning_prefix(self):
        return self._prefix('Warning', 'warn')

    def debug_prefix(self):
        return self._prefix('Debug', 'debug')

    def _with_progress(self, func, msg):
        from dvc.progress import progress
        with progress:
            func(msg)

    def _with_exc(self, func, prefix, msg, suffix="", exc=None):
        l_str_exc, str_tb = self.parse_exc(exc)

        if exc is not None and self.is_verbose():
            str_tb = str_tb if str_tb else traceback.format_exc()
            self._with_progress(self.logger.error, str_tb)

        l_msg = [prefix]
        if msg is not None and len(msg) != 0:
            l_msg.append(msg)
        l_msg += l_str_exc

        self._with_progress(func, ': '.join(l_msg) + suffix)

    def error(self, msg, exc=None):
        if self.is_quiet():
            return

        chat = "\n\nHaving any troubles? Hit us up at dvc.org/support, " \
               "we are always happy to help!"
        self._with_exc(self.logger.error,
                       self.error_prefix(),
                       msg,
                       suffix=chat,
                       exc=exc)

    def warn(self, msg, exc=None):
        if self.is_quiet():
            return

        self._with_exc(self.logger.warning,
                       self.warning_prefix(),
                       msg,
                       exc=exc)

    def debug(self, msg, exc=None):
        if not self.is_verbose():
            return

        self._with_exc(self.logger.debug,
                       self.debug_prefix(),
                       msg,
                       exc=exc)

    def info(self, msg):
        if self.is_quiet():
            return

        self._with_progress(self.logger.info, msg)

    def is_quiet(self):
        return self.lvl == logging.CRITICAL

    def is_verbose(self):
        return self.lvl == logging.DEBUG

    def box(self, msg, border_color=''):
        if self.is_quiet():
            return

        lines = msg.split('\n')
        max_width = max(visual_width(line) for line in lines)

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
                border=self.colorize(chars['vertical'], border_color),
                space=chars['empty'] * box_size_horizontal,
            ) * padding_vertical
        ]

        content_lines = [
            "{border}{space}{content}{space}{border}\n".format(
                border=self.colorize(chars['vertical'], border_color),
                space=chars['empty'] * padding_horizontal,
                content=visual_center(line, max_width),
            ) for line in lines
        ]

        box = "{margin}{padding}{content}{padding}{margin}".format(
                margin=self.colorize(margin, border_color),
                padding=''.join(padding_lines),
                content=''.join(content_lines),
            )

        print(box)

    def _already_initialized(self):
        return bool(self.logger.handlers)


logger = Logger()
