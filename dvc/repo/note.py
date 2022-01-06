import logging
import os

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.utils.serialize import dump_yaml, load_yaml

logger = logging.getLogger(__name__)
DVC_FILE_SUFFIX = ".dvc"
META_KEY = "meta"
NOTES_KEY = "notes"


@locked
def note(self, action=None, targets=[], key=None, value=None):
    if not targets:
        raise DvcException("No files specified.")

    target_filename = [get_dvc_filename(target) for target in targets]

    if action == "find":
        require("find", key)
        result = []
        for (target, filename) in target_filename:
            temp = find_key_uses(filename, key)
            if temp is not None:
                key, value = temp
                result.append([target, key, value])
        return result

    elif action == "list":
        result = []
        for (target, filename) in target_filename:
            result.append([target, list_keys(filename)])
        return result

    elif action == "remove":
        require("remove", key)
        changes = {}
        for (target, filename) in target_filename:
            temp = remove_key(filename, key)
            if temp is not None:
                changes[(target, filename)] = temp
        save_all(changes)

    elif action == "set":
        require("set", key, value)
        changes = {}
        for (target, filename) in target_filename:
            changes[(target, filename)] = set_key_value(filename, key, value)
        save_all(changes)

    else:
        raise DvcException(f"unknown command {action}")


def find_key_uses(filename, key):
    _, notes = get_data_and_notes(filename)
    if (notes is None) or (key not in notes):
        return None
    return [key, notes[key]]


def list_keys(filename):
    _, notes = get_data_and_notes(filename)
    if notes is None:
        return [filename, []]
    return list(notes.keys())


def remove_key(filename, key):
    data, notes = get_data_and_notes(filename)
    if (notes is None) or (key not in notes):
        return None
    del notes[key]
    if not notes:
        del data[META_KEY][NOTES_KEY]
    return data


def set_key_value(filename, key, value):
    data, notes = get_data_and_notes(filename, create=True)
    if key in notes:
        raise DvcException(f"{filename} already contains {key}")
    notes[key] = value
    return data


def require(command, key, *args):
    msg = None
    if not key:
        msg = f"command {command} requires key"
    if (len(args) == 1) and (not args[0]):
        if msg:
            msg += " and value"
        else:
            msg = f"command {command} requires value"
    if msg:
        raise DvcException(msg)


def get_dvc_filename(target):
    filename = target
    if not filename.endswith(DVC_FILE_SUFFIX):
        filename = filename + DVC_FILE_SUFFIX
    if not os.path.isfile(filename):
        raise DvcException(f"No such DVC file {filename}")
    return (target, filename)


def get_data_and_notes(filename, create=False):
    data = load_yaml(filename)
    if META_KEY in data:
        if NOTES_KEY in data[META_KEY]:
            return data, data[META_KEY][NOTES_KEY]
        elif create:
            data[META_KEY][NOTES_KEY] = {}
            return data, data[META_KEY][NOTES_KEY]
        else:
            return data, None
    elif create:
        data[META_KEY] = {NOTES_KEY: {}}
        return data, data[META_KEY][NOTES_KEY]
    else:
        return data, None


def save_data(filename, data):
    dump_yaml(filename, data)


def save_all(changes):
    for ((target, filename), data) in changes.items():
        save_data(filename, data)
