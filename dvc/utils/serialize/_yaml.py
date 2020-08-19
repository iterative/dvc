import io
from collections import OrderedDict

from funcy import reraise
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ._common import ParseError


class YAMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "YAML file structure is corrupted")


def load_yaml(path):
    with open(path, encoding="utf-8") as fd:
        return parse_yaml(fd.read(), path)


def parse_yaml(text, path, typ="safe"):
    yaml = YAML(typ=typ)
    with reraise(YAMLError, YAMLFileCorruptedError(path)):
        return yaml.load(text) or {}


def parse_yaml_for_update(text, path):
    """Parses text into Python structure.

    Unlike `parse_yaml()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_yaml()`.
    """
    return parse_yaml(text, path, typ="rt")


def _get_yaml():
    yaml = YAML()
    yaml.default_flow_style = False

    # tell Dumper to represent OrderedDict as normal dict
    yaml_repr_cls = yaml.Representer
    yaml_repr_cls.add_representer(OrderedDict, yaml_repr_cls.represent_dict)
    return yaml


def dump_yaml(path, data):
    yaml = _get_yaml()
    with open(path, "w+", encoding="utf-8") as fd:
        yaml.dump(data, fd)


def loads_yaml(s):
    return YAML(typ="safe").load(s)


def dumps_yaml(d):
    stream = io.StringIO()
    YAML().dump(d, stream)
    return stream.getvalue()
