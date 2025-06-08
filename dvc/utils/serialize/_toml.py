from contextlib import contextmanager

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class TOMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "TOML file structure is corrupted")


def load_toml(path, fs=None, **kwargs):
    return _load_data(path, parser=parse_toml, fs=fs, **kwargs)


def _parse_toml(text, path):
    from tomlkit import loads
    from tomlkit.exceptions import ParseError as TomlkitParseError

    with reraise(TomlkitParseError, TOMLFileCorruptedError(path)):
        return loads(text)


def parse_toml(text, path, preserve_comments=False):
    rval = _parse_toml(text, path)

    if preserve_comments:
        return rval

    return rval.unwrap()


def parse_toml_for_update(text, path):
    return parse_toml(text, path, preserve_comments=True)


def _dump(data, stream, sort_keys=False):
    import tomlkit

    return tomlkit.dump(data, stream, sort_keys=sort_keys)


def dump_toml(path, data, fs=None, **kwargs):
    return _dump_data(path, data, dumper=_dump, fs=fs, **kwargs)


@contextmanager
def modify_toml(path, fs=None):
    with _modify_data(path, parse_toml_for_update, _dump, fs=fs) as d:
        yield d
