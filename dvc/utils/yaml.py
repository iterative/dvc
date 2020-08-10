from collections import OrderedDict

from ruamel.yaml import YAML
from ruamel.yaml.emitter import Emitter
from ruamel.yaml.error import YAMLError
from ruamel.yaml.events import DocumentStartEvent

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


class YAMLEmitterNoVersionDirective(Emitter):
    MARKER_START_LINE = "---"

    def write_version_directive(self, version_text):
        """Do not write version directive at all."""

    # pylint: disable=signature-differs
    def write_indicator(self, indicator, *args, **kwargs):
        if isinstance(self.event, DocumentStartEvent):
            # TODO: need more tests, how reliable is this check?
            skip_marker = (
                not self.event.explicit
                and not self.canonical
                and not self.event.tags
            )
            # FIXME: if there is a marker for "% YAML 1.1", it might
            #  get removed
            if skip_marker and indicator == self.MARKER_START_LINE:
                # skip adding marker line
                return
        super().write_indicator(indicator, *args, **kwargs)


def dump_yaml(path, data):
    with open(path, "w", encoding="utf-8") as fd:

        yaml = YAML()
        # dump by default in v1.1
        yaml.version = (1, 1)
        yaml.default_flow_style = False
        # skip printing directive, and also skip marker line for document start
        yaml.Emitter = YAMLEmitterNoVersionDirective
        # tell Dumper to represent OrderedDict as a normal dict
        yaml.Representer.add_representer(
            OrderedDict, yaml.Representer.represent_dict
        )
        yaml.dump(data, fd)
