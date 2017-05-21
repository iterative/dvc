import logging

class Logger(object):
    DEFAULT_LEVEL = logging.INFO

    LEVEL_MAP = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARNING,
        'error': logging.ERROR
    }

    logging.basicConfig(stream=sys.stdout, format='%(message)s', level=DEFAULT_LEVEL)

    _logger = logging.getLogger('dvc')

    @staticmethod
    def set_level(level):
        Logger._logger.setLevel(Logger.LEVEL_MAP.get(level.lower(), 'debug'))

    @staticmethod
    def be_quiet():
        Logger._logger.setLevel(logging.CRITICAL)

    @staticmethod
    def be_verbose():
        Logger._logger.setLevel(logging.DEBUG)

    @staticmethod
    def error(msg):
        return Logger._logger.error(msg)

    @staticmethod
    def warn(msg):
        return Logger._logger.warn(msg)

    @staticmethod
    def debug(msg):
        return Logger._logger.debug(msg)

    @staticmethod
    def info(msg):
        return Logger._logger.info(msg) 
