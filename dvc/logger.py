class Logger(object):
    DEFAULT_LEVEL = 2

    LEVEL = DEFAULT_LEVEL

    LEVEL_MAP = {
        'debug': 1,
        'info': 2,
        'warn': 3,
        'error': 4
    }

    @staticmethod
    def set_level(level):
        Logger.LEVEL = Logger.LEVEL_MAP.get(level.lower(), 'debug')

    @staticmethod
    def is_debug():
        return Logger.LEVEL <= 1

    @staticmethod
    def is_info():
        return Logger.LEVEL <= 2

    @staticmethod
    def is_warn():
        return Logger.LEVEL <= 3

    @staticmethod
    def is_error():
        return Logger.LEVEL <= 4

    @staticmethod
    def debug(msg):
        if Logger.is_debug():
            print(u'Debug. {}'.format(msg))

    @staticmethod
    def info(msg):
        if Logger.is_info():
            #print(u'Info. {}'.format(msg))
            print(msg)

    @staticmethod
    def warn(msg):
        if Logger.is_warn():
            print(u'Warning. {}'.format(msg))

    @staticmethod
    def error(msg):
        if Logger.is_error():
            print(u'Error. {}'.format(msg))
