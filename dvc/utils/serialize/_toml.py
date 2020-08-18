import toml
from funcy import reraise

from ._common import ParseError


class TOMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "TOML file structure is corrupted")


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


def dump_toml(path, data):
    with open(path, "w+", encoding="utf-8") as fobj:
        toml.dump(data, fobj, encoder=toml.TomlPreserveCommentEncoder())
