from collections import OrderedDict

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from dvc.exceptions import YAMLFileCorruptedError

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


def load_yaml(path):
    with open(path, encoding="utf-8") as fd:
        return parse_yaml(fd.read(), path)


def parse_yaml(text, path):
    try:
        import yaml

        return yaml.load(text, Loader=SafeLoader) or {}
    except yaml.error.YAMLError as exc:
        raise YAMLFileCorruptedError(path) from exc


def parse_yaml_for_update(text, path):
    """Parses text into Python structure.

    Unlike `parse_yaml()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_yaml()`.
    """
    try:
        yaml = YAML()
        return yaml.load(text) or {}
    except YAMLError as exc:
        raise YAMLFileCorruptedError(path) from exc


def dump_yaml(path, data):
    with open(path, "w", encoding="utf-8") as fd:
        yaml = YAML()
        yaml.default_flow_style = False
        # tell Dumper to represent OrderedDict as
        # normal dict
        yaml.Representer.add_representer(
            OrderedDict, yaml.Representer.represent_dict
        )
        yaml.dump(data, fd)
