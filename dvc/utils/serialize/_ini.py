import json
import re
from ast import literal_eval
from contextlib import contextmanager
from typing import Any, Dict

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class ConfigFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "Config file structure is corrupted")


def split_path(path: str):
    offset = 0
    result = []
    for match in re.finditer(r"(?:'([^']*)'|\"([^\"]*)\"|([^.]*))(?:[.]|$)", path):
        assert match.start() == offset, f"Malformed path: {path!r} in config"
        offset = match.end()
        result.append(next(g for g in match.groups() if g is not None))
        if offset == len(path):
            break
    return result


def join_path(path):
    # This is required to handle sections like `[foo."bar.baz".qux]`
    return ".".join(repr(x) if "." in x else x for x in path)


def config_literal_eval(s: str):
    try:
        return literal_eval(s)
    except (ValueError, SyntaxError):
        try:
            return json.loads(s)
        except ValueError:
            return s


def config_literal_dump(v: Any):
    if isinstance(v, str) and config_literal_eval(str(v)) == v:
        return str(v)
    return json.dumps(v)


def flatten_sections(root: Dict[str, Any]) -> Dict[str, Any]:
    res: Dict = {}

    def rec(d, path):
        res.setdefault(join_path(path), {})
        section = {}
        for k, v in d.items():
            if isinstance(v, dict):
                rec(v, (*path, k))
            else:
                section[k] = v
        res[join_path(path)].update(section)

    rec(root, ())
    res.pop("", None)
    return dict(res)


def load_ini(path, fs=None, **kwargs):
    return _load_data(path, parser=parse_ini, fs=fs)


def parse_ini(text, path, **kwargs):
    import configparser

    with reraise(configparser.Error, ConfigFileCorruptedError(path)):
        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str  # type: ignore[assignment,method-assign]
        parser.read_string(text)
        config: Dict = {}
        for section in parser.sections():
            parts = split_path(section)
            current = config
            for part in parts:
                if part not in current:
                    current[part] = current = {}
                else:
                    current = current[part]
            current.update(
                {k: config_literal_eval(v) for k, v in parser.items(section)}
            )

    return config


def _dump(data, stream):
    import configparser

    prepared = flatten_sections(data)

    parser = configparser.ConfigParser(interpolation=None)

    parser.optionxform = str  # type: ignore[assignment,method-assign]
    for section_name, section in prepared.items():
        content = {k: config_literal_dump(v) for k, v in section.items()}
        if content:
            parser.add_section(section_name)
            parser[section_name].update(content)

    return parser.write(stream)


def dump_ini(path, data, fs=None, **kwargs):
    return _dump_data(path, data, dumper=_dump, fs=fs, **kwargs)


@contextmanager
def modify_ini(path, fs=None):
    """
    NOTE: As configparser does not parse comments, those will be striped
    from the modified config file
    """
    with _modify_data(path, parse_ini, _dump, fs=fs) as d:
        yield d
