from contextlib import contextmanager

import toml
from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class TOMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "TOML file structure is corrupted")


def load_toml(path, tree=None):
    return _load_data(path, parser=parse_toml, tree=tree)


def parse_toml(text, path, decoder=None):
    with reraise(toml.TomlDecodeError, TOMLFileCorruptedError(path)):
        return toml.loads(text, decoder=decoder)


def parse_toml_for_update(text, path):
    """Parses text into Python structure.

    NOTE: Python toml package does not currently use ordered dicts, so
    keys may be re-ordered between load/dump, but this function will at
    least preserve comments.
    """
    decoder = toml.TomlPreserveCommentDecoder()
    return parse_toml(text, path, decoder=decoder)


def _dump(data, stream):
    return toml.dump(data, stream, encoder=toml.TomlPreserveCommentEncoder())


def dump_toml(path, data, tree=None):
    return _dump_data(path, data, dumper=_dump, tree=tree)


@contextmanager
def modify_toml(path, tree=None):
    with _modify_data(path, parse_toml_for_update, dump_toml, tree=tree) as d:
        yield d
