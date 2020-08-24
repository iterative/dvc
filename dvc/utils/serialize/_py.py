import inspect
from contextlib import contextmanager

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class PYFileCorruptedError(ParseError):
    def __init__(self, path, message="PY file structure is corrupted"):
        super().__init__(path, message)


def load_py(path, tree=None):
    return _load_data(path, parser=parse_py, tree=tree)


def parse_py(text, path):
    """Parses text from .py file into Python structure."""
    module = dict()
    with reraise(SyntaxError, PYFileCorruptedError(path)):
        exec(text, module)
    with reraise(
        KeyError, PYFileCorruptedError(path, "PY file is missing class Config")
    ):
        config = module["Config"]()

    config_dict = parse_class(config, module)
    return config_dict


def parse_class(cls, module_dict):
    """Parses class attributes values to dict."""
    attrs = inspect.getmembers(cls, lambda a: not (inspect.isroutine(a)))

    cfg_dict = dict()
    for attr, value in attrs:
        if not (attr.startswith("__") and attr.endswith("__")):
            for mname, mvalue in module_dict.items():
                if inspect.isclass(mvalue) and isinstance(value, mvalue):
                    value = parse_class(value, module_dict)
            cfg_dict[attr] = value
    return cfg_dict


def parse_py_for_update(text, path):
    return parse_py(text, path)


def _dump(data, stream):
    """Save dict as .py file."""
    return NotImplemented


def dump_py(path, data, tree=None):
    return _dump_data(path, data, dumper=_dump, tree=tree)


@contextmanager
def modify_py(path, tree=None):
    with _modify_data(path, parse_py_for_update, dump_py, tree=tree) as d:
        yield d
