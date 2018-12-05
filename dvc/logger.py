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

    def __init__(self, loglevel=None):
        if loglevel:
            Logger.set_level(loglevel)

    @staticmethod
    def init():

        class LogLevelFilter(logging.Filter):
            def filter(self, record):
                return record.levelno <= logging.WARNING

        if Logger._already_initialized():
            return

        sh_out = logging.StreamHandler(sys.stdout)
        sh_out.setFormatter(logging.Formatter(Logger.FMT))
        sh_out.setLevel(logging.DEBUG)
        sh_out.addFilter(LogLevelFilter())

        sh_err = logging.StreamHandler(sys.stderr)
        sh_err.setFormatter(logging.Formatter(Logger.FMT))
        sh_err.setLevel(logging.ERROR)

        Logger.logger().addHandler(sh_out)
        Logger.logger().addHandler(sh_err)
        Logger.set_level()

    @staticmethod
    def logger():
        return logging.getLogger('dvc')

    @staticmethod
    def set_level(level=None):
        if not level:
            lvl = Logger.DEFAULT_LEVEL
        else:
            lvl = Logger.LEVEL_MAP.get(level.lower(), Logger.DEFAULT_LEVEL)
        Logger.logger().setLevel(lvl)

    @staticmethod
    def be_quiet():
        Logger.logger().setLevel(logging.CRITICAL)

    @staticmethod
    def be_verbose():
        Logger.logger().setLevel(logging.DEBUG)

    @staticmethod
    def colorize(msg, color):
        header = ''
        footer = ''

        if sys.stdout.isatty():  # pragma: no cover
            header = Logger.COLOR_MAP.get(color.lower(), '')
            footer = colorama.Style.RESET_ALL

        return u'{}{}{}'.format(header, msg, footer)

    @staticmethod
    def parse_exc(exc, tb=None):
        str_tb = tb if tb else None
        str_exc = str(exc) if exc else ""
        l_str_exc = []

        if len(str_exc) != 0:
            l_str_exc.append(str_exc)

        if exc and hasattr(exc, 'cause') and exc.cause:
            cause_tb = exc.cause_tb if hasattr(exc, 'cause_tb') else None
            l_cause_str_exc, cause_str_tb = Logger.parse_exc(exc.cause,
                                                             cause_tb)

            str_tb = cause_str_tb
            l_str_exc += l_cause_str_exc

        return (l_str_exc, str_tb)

    @staticmethod
    def _prefix(msg, typ):
        color = Logger.LEVEL_COLOR_MAP.get(typ.lower(), '')
        return Logger.colorize('{}'.format(msg), color)

    @staticmethod
    def error_prefix():
        return Logger._prefix('Error', 'error')

    @staticmethod
    def warning_prefix():
        return Logger._prefix('Warning', 'warn')

    @staticmethod
    def debug_prefix():
        return Logger._prefix('Debug', 'debug')

    @staticmethod
    def _with_progress(func, msg):
        from dvc.progress import progress
        with progress:
            func(msg)

    @staticmethod
    def _with_exc(func, prefix, msg, suffix="", exc=None):
        l_str_exc, str_tb = Logger.parse_exc(exc)

        if exc is not None and Logger.is_verbose():
            str_tb = str_tb if str_tb else traceback.format_exc()
            Logger._with_progress(Logger.logger().error, str_tb)

        l_msg = [prefix]
        if msg is not None and len(msg) != 0:
            l_msg.append(msg)
        l_msg += l_str_exc

        Logger._with_progress(func, ': '.join(l_msg) + suffix)

    @staticmethod
    def error(msg, exc=None):
        chat = "\n\nHaving any troubles? Hit us up at dvc.org/support, " \
               "we are always happy to help!"
        Logger._with_exc(Logger.logger().error,
                         Logger.error_prefix(),
                         msg,
                         suffix=chat,
                         exc=exc)

    @classmethod
    def warn(cls, msg, exc=None):
        cls._with_exc(cls.logger().warning,
                      cls.warning_prefix(),
                      msg,
                      exc=exc)

    @classmethod
    def debug(cls, msg, exc=None):
        cls._with_exc(cls.logger().debug,
                      cls.debug_prefix(),
                      msg,
                      exc=exc)

    @staticmethod
    def info(msg):
        Logger._with_progress(Logger.logger().info, msg)

    @staticmethod
    def is_quiet():
        return Logger.logger().level == logging.CRITICAL

    @staticmethod
    def is_verbose():
        return Logger.logger().getEffectiveLevel() == logging.DEBUG

    @classmethod
    def box(cls, msg, border_color=''):
        if cls.is_quiet():
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
                border=cls.colorize(chars['vertical'], border_color),
                space=chars['empty'] * box_size_horizontal,
            ) * padding_vertical
        ]

        content_lines = [
            "{border}{space}{content}{space}{border}\n".format(
                border=cls.colorize(chars['vertical'], border_color),
                space=chars['empty'] * padding_horizontal,
                content=visual_center(line, max_width),
            ) for line in lines
        ]

        box = "{margin}{padding}{content}{padding}{margin}".format(
                margin=cls.colorize(margin, border_color),
                padding=''.join(padding_lines),
                content=''.join(content_lines),
            )

        print(box)

    @staticmethod
    def _already_initialized():
        return bool(Logger.logger().handlers)
