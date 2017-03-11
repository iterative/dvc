import sys


class Logger(object):
    @staticmethod
    def info(msg):
        print('{}'.format(msg))

    @staticmethod
    def warn(msg):
        print('{}'.format(msg))

    @staticmethod
    def error(msg):
        sys.stderr.write('{}\n'.format(msg))

    @staticmethod
    def verbose(msg):
        print('{}'.format(msg))