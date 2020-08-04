import toml

from dvc.exceptions import TOMLFileCorruptedError


def parse_toml_for_update(text, path):
    """Parses text into Python structure.

    NOTE: Python toml package does not currently use ordered dicts, so
    keys may be re-ordered between load/dump, but this function will at
    least preserve comments.
    """
    try:
        return toml.loads(text, decoder=toml.TomlPreserveCommentDecoder())
    except toml.TomlDecodeError as exc:
        raise TOMLFileCorruptedError(path) from exc


def dump_toml(path, data):
    with open(path, "w", encoding="utf-8") as fobj:
        toml.dump(data, fobj, encoder=toml.TomlPreserveCommentEncoder())
