from collections import defaultdict

from ._common import *  # noqa: F403
from ._json import *  # noqa: F403
from ._py import *  # noqa: F403
from ._toml import *  # noqa: F403
from ._yaml import *  # noqa: F403

LOADERS: defaultdict[str, LoaderFn] = defaultdict(  # noqa: F405
    lambda: load_yaml  # noqa: F405
)
LOADERS.update({".toml": load_toml, ".json": load_json, ".py": load_py})  # noqa: F405

PARSERS: defaultdict[str, ParserFn] = defaultdict(  # noqa: F405
    lambda: parse_yaml  # noqa: F405
)
PARSERS.update(
    {".toml": parse_toml, ".json": parse_json, ".py": parse_py}  # noqa: F405
)


def load_path(fs_path, fs, **kwargs):
    suffix = fs.suffix(fs_path).lower()
    loader = LOADERS[suffix]
    return loader(fs_path, fs=fs, **kwargs)


DUMPERS: defaultdict[str, DumperFn] = defaultdict(  # noqa: F405
    lambda: dump_yaml  # noqa: F405
)
DUMPERS.update({".toml": dump_toml, ".json": dump_json, ".py": dump_py})  # noqa: F405

MODIFIERS: defaultdict[str, ModifierFn] = defaultdict(  # noqa: F405
    lambda: modify_yaml  # noqa: F405
)
MODIFIERS.update(
    {
        ".toml": modify_toml,  # noqa: F405
        ".json": modify_json,  # noqa: F405
        ".py": modify_py,  # noqa: F405
    }
)
