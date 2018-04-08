import sys
import logging
import colorama
import traceback


colorama.init()


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
        'debug': colorama.Fore.BLUE,
        'warn': colorama.Fore.YELLOW,
        'error': colorama.Fore.RED
    }

    def __init__(self, loglevel=None):
        if loglevel:
            Logger.set_level(loglevel)

    @staticmethod
    def init():
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(Logger.FMT))
        sh.setLevel(logging.DEBUG)

        Logger.logger().addHandler(sh)
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
    def colorize(msg, typ):
        header = ''
        footer = ''

        if sys.stdout.isatty():
            header = Logger.COLOR_MAP.get(typ.lower(), '')
            footer = colorama.Style.RESET_ALL

        return u'{}{}{}'.format(header, msg, footer)

    @staticmethod
    def parse_exc(exc, tb=None):
        str_tb = tb if tb else None
        str_exc = ': {}'.format(str(exc)) if exc else ""

        if exc and hasattr(exc, 'cause') and exc.cause:
            cause_tb = exc.cause_tb if hasattr(exc, 'cause_tb') else None
            cause_str_exc, cause_str_tb = Logger.parse_exc(exc.cause, cause_tb)

            str_tb = cause_str_tb
            str_exc = '{}{}'.format(str_exc, cause_str_exc)

        return (str_exc, str_tb)

    @staticmethod
    def error(msg, exc=None):
        str_exc, str_tb = Logger.parse_exc(exc)
        if Logger.logger().getEffectiveLevel() == logging.DEBUG and exc:
            str_tb = str_tb if str_tb else traceback.format_exc()
            Logger.logger().error(str_tb)
        return Logger.logger().error(Logger.colorize(msg + str_exc, 'error'))

    @staticmethod
    def warn(msg):
        return Logger.logger().warn(Logger.colorize(msg, 'warn'))

    @staticmethod
    def debug(msg):
        return Logger.logger().debug(Logger.colorize(msg, 'debug'))

    @staticmethod
    def info(msg):
        return Logger.logger().info(Logger.colorize(msg, 'info'))
