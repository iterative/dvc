"""
python-benedict style keypath parsing.
Adapted from https://github.com/fabiocaccamo/python-benedict/blob/c\
98c471065ae84b4752a87f1bd63fe3987783663/benedict/dicts/keypath/keypath_util.py
"""

import re

KEY_INDEX_RE = r"(?:\[[\'\"]*(\-?[\d]+)[\'\"]*\]){1}$"


def parse_keypath(keypath, separator):
    """
    Splits keys and indexes using the given separator:
    eg. 'item[0].subitem[1]' -> ['item', 0, 'subitem', 1].
    """
    keys1 = _split_keys(keypath, separator)
    keys2 = []
    for key in keys1:
        keys2 += _split_key_indexes(key)
    return keys2


def _split_key_indexes(key):
    """
    Splits key indexes:
    eg. 'item[0][1]' -> ['item', 0, 1].
    """
    if "[" in key and key.endswith("]"):
        keys = []
        while True:
            matches = re.findall(KEY_INDEX_RE, key)
            if matches:
                key = re.sub(KEY_INDEX_RE, "", key)
                index = int(matches[0])
                keys.insert(0, index)
                # keys.insert(0, { keylist_util.INDEX_KEY:index })
                continue
            keys.insert(0, key)
            break
        return keys
    return [key]


def _split_keys(keypath, separator):
    """
    Splits keys using the given separator:
    eg. 'item.subitem[1]' -> ['item', 'subitem[1]'].
    """
    if separator:
        return keypath.split(separator)
    return [keypath]
