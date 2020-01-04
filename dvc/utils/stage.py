import yaml
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from dvc.exceptions import StageFileCorruptedError


def load_stage_file(path):
    with open(path, "r", encoding="utf-8") as fd:
        return parse_stage(fd.read(), path)


def parse_stage(text, path):
    try:
        return yaml.load(text, Loader=SafeLoader) or {}
    except yaml.error.YAMLError as exc:
        raise StageFileCorruptedError(path) from exc


def parse_stage_for_update(text, path):
    """Parses text into Python structure.

    Unlike `parse_stage()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_stage()`.
    """
    try:
        yaml = YAML()
        return yaml.load(text) or {}
    except YAMLError as exc:
        raise StageFileCorruptedError(path) from exc


def dump_stage_file(path, data):
    with open(path, "w", encoding="utf-8") as fd:
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.dump(data, fd)
