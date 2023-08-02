from collections import defaultdict
from typing import DefaultDict

from ._common import *  # noqa: F403, pylint: disable=wildcard-import
from ._json import *  # noqa: F403, pylint: disable=wildcard-import
from ._py import *  # noqa: F403, pylint: disable=wildcard-import
from ._toml import *  # noqa: F403, pylint: disable=wildcard-import
from ._yaml import *  # noqa: F403, pylint: disable=wildcard-import

LOADERS: DefaultDict[str, LoaderFn] = defaultdict(  # noqa: F405
    lambda: load_yaml  # noqa: F405
)
LOADERS.update({".toml": load_toml, ".json": load_json, ".py": load_py})  # noqa: F405

PARSERS: DefaultDict[str, ParserFn] = defaultdict(  # noqa: F405
    lambda: parse_yaml  # noqa: F405
)
PARSERS.update(
    {".toml": parse_toml, ".json": parse_json, ".py": parse_py}  # noqa: F405
)


def load_path(fs_path, fs):
    suffix = fs.path.suffix(fs_path).lower()
    loader = LOADERS[suffix]
    return loader(fs_path, fs=fs)


DUMPERS: DefaultDict[str, DumperFn] = defaultdict(  # noqa: F405
    lambda: dump_yaml  # noqa: F405
)
DUMPERS.update({".toml": dump_toml, ".json": dump_json, ".py": dump_py})  # noqa: F405

MODIFIERS: DefaultDict[str, ModifierFn] = defaultdict(  # noqa: F405
    lambda: modify_yaml  # noqa: F405
)
MODIFIERS.update(
    {
        ".toml": modify_toml,  # noqa: F405
        ".json": modify_json,  # noqa: F405
        ".py": modify_py,  # noqa: F405
    }
)
