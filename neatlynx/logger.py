import sys


class Logger(object):
    @staticmethod
    def printing(msg):
        print(u'{}'.format(msg))

    @staticmethod
    def warn(msg):
        print(u'Warning. {}'.format(msg))

    @staticmethod
    def error(msg):
        #sys.stderr.write('Error. {}\n'.format(msg))
        print(u'Error. {}'.format(msg))

    @staticmethod
    def debug(msg):
        print(u'Debug. {}'.format(msg))
