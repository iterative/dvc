import json

from funcy import contextmanager, reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class JSONFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "JSON file structure is corrupted")


def load_json(path, tree=None):
    return _load_data(path, parser=parse_json, tree=tree)


def parse_json(text, path, **kwargs):
    with reraise(json.JSONDecodeError, JSONFileCorruptedError(path)):
        return json.loads(text, **kwargs) or {}


def dump_json(path, data, tree=None):
    return _dump_data(path, data, dumper=json.dump, tree=tree)


@contextmanager
def modify_json(path, tree=None):
    with _modify_data(path, parse_json, dump_json, tree=tree) as d:
        yield d
