import sys
from getpass import getpass

try:
    # NOTE: in Python3 raw_input() was renamed to input()
    input = raw_input
except NameError:
    pass


def ask(prompt, limited_to=None):
    if not sys.stdout.isatty():
        return

    while True:
        answer = input(prompt + ' ').lower()

        if not limited_to:
            return answer

        if answer in limited_to:
            return answer

        print("Your response must be one of: {options}. Please try again."
              .format(options=limited_to))


def confirm(statement):
    prompt = '{statement} [y/n]'.format(statement=statement)
    answer = ask(prompt, limited_to=['yes', 'no', 'y', 'n'])
    return answer and answer.startswith('y')


def password(statement):
    prompt = '{statement}: '.format(statement=statement)
    return getpass(prompt)
