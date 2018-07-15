import sys

try:
    # NOTE: in Python3 raw_input() was renamed to input()
    input = raw_input
except NameError:
    pass


def prompt(msg, default=False):
    if not sys.stdout.isatty():
        return default

    answer = input(msg + u'\n').lower()
    while answer not in ['yes', 'no', 'y', 'n']:
        answer = input('Enter \'yes\' or \'no\'.\n').lower()

    return answer[0] == "y"
