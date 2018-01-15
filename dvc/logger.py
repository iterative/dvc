import sys
import logging
import colorama

from dvc.config import Config

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

    def __init__(self, config=None):
        if config:
            level = config[Config.SECTION_GLOBAL].get('LogLevel', None)
            Logger.set_level(level)

    @staticmethod
    def init(config=None):
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
    def error(msg, **kwargs):
        exc_info = Logger.logger().getEffectiveLevel() == logging.DEBUG
        return Logger.logger().error(Logger.colorize(msg, 'error'), exc_info=exc_info, **kwargs)

    @staticmethod
    def warn(msg, **kwargs):
        return Logger.logger().warn(Logger.colorize(msg, 'warn'), **kwargs)

    @staticmethod
    def debug(msg, **kwargs):
        return Logger.logger().debug(Logger.colorize(msg, 'debug'), **kwargs)

    @staticmethod
    def info(msg, **kwargs):
        return Logger.logger().info(Logger.colorize(msg, 'info'), **kwargs)
