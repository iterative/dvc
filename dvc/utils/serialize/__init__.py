from collections import defaultdict
from typing import DefaultDict

from ._common import *  # noqa: F403
from ._ini import *  # noqa: F403
from ._json import *  # noqa: F403
from ._py import *  # noqa: F403
from ._toml import *  # noqa: F403
from ._yaml import *  # noqa: F403

LOADERS: DefaultDict[str, LoaderFn] = defaultdict(  # noqa: F405
    lambda: load_yaml  # noqa: F405
)
LOADERS.update(
    {
        ".toml": load_toml,  # noqa: F405
        ".json": load_json,  # noqa: F405
        ".py": load_py,  # noqa: F405
        ".cfg": load_ini,  # noqa: F405
        ".ini": load_ini,  # noqa: F405
    }
)

PARSERS: DefaultDict[str, ParserFn] = defaultdict(  # noqa: F405
    lambda: parse_yaml  # noqa: F405
)
PARSERS.update(
    {
        ".toml": parse_toml,  # noqa: F405
        ".json": parse_json,  # noqa: F405
        ".py": parse_py,  # noqa: F405
        ".cfg": parse_ini,  # noqa: F405
        ".ini": parse_ini,  # noqa: F405
    }
)


def load_path(fs_path, fs, **kwargs):
    suffix = fs.suffix(fs_path).lower()
    loader = LOADERS[suffix]
    return loader(fs_path, fs=fs, **kwargs)


DUMPERS: DefaultDict[str, DumperFn] = defaultdict(  # noqa: F405
    lambda: dump_yaml  # noqa: F405
)
DUMPERS.update(
    {
        ".toml": dump_toml,  # noqa: F405
        ".json": dump_json,  # noqa: F405
        ".py": dump_py,  # noqa: F405
        ".cfg": dump_ini,  # noqa: F405
        ".ini": dump_ini,  # noqa: F405
    }
)

MODIFIERS: DefaultDict[str, ModifierFn] = defaultdict(  # noqa: F405
    lambda: modify_yaml  # noqa: F405
)
MODIFIERS.update(
    {
        ".toml": modify_toml,  # noqa: F405
        ".json": modify_json,  # noqa: F405
        ".py": modify_py,  # noqa: F405
        ".cfg": modify_ini,  # noqa: F405
        ".ini": modify_ini,  # noqa: F405
    }
)
