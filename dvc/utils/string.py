import re
from difflib import get_close_matches


def fuzzy_match(string: str, dictionary: list) -> list:
    """ Performs fuzzy match of the string with the given dictionary
    and returns the list of possible best "good enough" matches
    """
    if not string or not dictionary:
        return []

    return get_close_matches(
        word=string, possibilities=dictionary, n=3, cutoff=0.6
    )


def regex_search(string: str, regex: str) -> str:
    """ Search using regex on string and return the first match"""
    match = re.search(regex, string)
    if not match:
        return ""
    return match.group(1)
