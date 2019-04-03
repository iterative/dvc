"""Manages user prompts."""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import logging
from getpass import getpass

from dvc.utils.compat import input


logger = logging.getLogger(__name__)


def _ask(prompt, limited_to=None):
    if not sys.stdout.isatty():
        return None

    while True:
        try:
            answer = input(prompt + " ").lower()
        except EOFError:
            return None

        if not limited_to:
            return answer

        if answer in limited_to:
            return answer

        logger.info(
            "Your response must be one of: {options}. "
            "Please try again.".format(options=limited_to)
        )


def confirm(statement):
    """Ask the user for confirmation about the specified statement.

    Args:
        statement (unicode): statement to ask the user confirmation about.

    Returns:
        bool: whether or not specified statement was confirmed.
    """
    prompt = "{statement} [y/n]".format(statement=statement)
    answer = _ask(prompt, limited_to=["yes", "no", "y", "n"])
    return answer and answer.startswith("y")


def password(statement):
    """Ask the user for a password.

    Args:
        statement (str): string to prompt the user with.

    Returns:
        str: password entered by the user.
    """
    logger.info("{statement}: ".format(statement=statement))
    return getpass("")
