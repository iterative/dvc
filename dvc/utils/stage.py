from dvc.utils.compat import open

from ruamel.yaml.error import YAMLError
from ruamel.yaml import YAML

from dvc.exceptions import StageFileCorruptedError


def load_stage_file(path):
    with open(path, "r", encoding="utf-8") as fd:
        return load_stage_fd(fd, path)


def load_stage_fd(fd, path):
    try:
        yaml = YAML()
        return yaml.load(fd) or {}
    except YAMLError as exc:
        raise StageFileCorruptedError(path, cause=exc)


def dump_stage_file(path, data):
    with open(path, "w", encoding="utf-8") as fd:
        yaml = YAML()
        yaml.default_flow_style = False
        yaml.dump(data, fd)
