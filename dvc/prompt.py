"""Manages user prompts."""

import logging
import sys
from getpass import getpass
from typing import Collection, Optional

logger = logging.getLogger(__name__)


def ask(prompt: str, limited_to: Optional[Collection[str]] = None):
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

        logger.info("Your response must be one of: %s. Please try again.", limited_to)


def confirm(statement: str) -> bool:
    """Ask the user for confirmation about the specified statement.

    Args:
        statement (unicode): statement to ask the user confirmation about.

    Returns:
        bool: whether or not specified statement was confirmed.
    """
    prompt = f"{statement} [y/n]"
    answer = ask(prompt, limited_to=["yes", "no", "y", "n"])
    return answer and answer.startswith("y")


def password(statement: str) -> str:
    """Ask the user for a password.

    Args:
        statement (str): string to prompt the user with.

    Returns:
        str: password entered by the user.
    """
    logger.info("%s: ", statement)
    return getpass("")
