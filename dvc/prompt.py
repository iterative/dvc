import sys

from dvc.progress import progress

try:
    # NOTE: in Python3 raw_input() was renamed to input()
    input = raw_input
except NameError:
    pass


class Prompt(object):
    def __init__(self):
        self.default = None

    def prompt(self, msg, default=False):  # pragma: no cover
        if self.default is not None:
            return self.default

        if not sys.stdout.isatty():
            return default

        answer = input(msg + u' (y/n)\n').lower()
        while answer not in ['yes', 'no', 'y', 'n']:
            answer = input('Enter \'yes\' or \'no\'.\n').lower()

        return answer[0] == "y"

    def prompt_password(self, msg):  # pragma: no cover
        import getpass

        if not sys.stdout.isatty():
            return None

        msg = 'Enter password for {}:\n'.format(msg)

        if not progress.is_finished:
            msg = u'\n' + msg

        return getpass.getpass(msg)
