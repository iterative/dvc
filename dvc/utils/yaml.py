from collections import OrderedDict

from funcy import reraise
from ruamel.yaml import YAML
from ruamel.yaml.emitter import Emitter
from ruamel.yaml.error import YAMLError
from ruamel.yaml.events import DocumentStartEvent

from dvc.exceptions import YAMLFileCorruptedError

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


class YAMLVersion:
    V11 = (1, 1)
    V12 = (1, 2)


def _parse_yaml_v1_1(text, path):
    import yaml

    with reraise(yaml.error.YAMLError, YAMLFileCorruptedError(path)):
        return yaml.load(text, Loader=SafeLoader) or {}


def _parse_yaml_v1_2(text, path):
    yaml = YAML(typ="safe")
    yaml.version = YAMLVersion.V12
    with reraise(YAMLError, YAMLFileCorruptedError(path)):
        return yaml.load(text) or {}


def parse_yaml(text, path, *, version=None):
    parser = _parse_yaml_v1_1
    if version == YAMLVersion.V12:
        parser = _parse_yaml_v1_2
    return parser(text, path)


def load_yaml(path, *, version=None):
    with open(path, encoding="utf-8") as fd:
        return parse_yaml(fd.read(), path, version=version)


def parse_yaml_for_update(text, path, *, version=YAMLVersion.V11):
    """Parses text into Python structure.

    Unlike `parse_yaml()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_yaml()`.
    """
    yaml = YAML()
    yaml.version = version
    with reraise(YAMLError, YAMLFileCorruptedError(path)):
        return yaml.load(text) or {}


class _YAMLEmitterNoVersionDirective(Emitter):
    """
    This emitter skips printing version directive when we set yaml version
    on `dump_yaml()`. Also, ruamel.yaml will still try to add a document start
    marker line (assuming version directive was written), for which we
    need to find a _hack_ to ensure the marker line is not written to the
    stream, as our dvcfiles and hopefully, params file are single document
    YAML files.

    NOTE: do not use this emitter during load/parse, only when dump for 1.1
    """

    MARKER_START_LINE = "---"

    def write_version_directive(self, version_text):
        """Do not write version directive at all."""

    def expect_first_document_start(self):
        # as our yaml files are expected to only have a single document,
        # this is not needed, just trying to make it a bit resilient,
        # but it's not well-thought out.
        # pylint: disable=attribute-defined-outside-init
        self._first_document = True
        ret = super().expect_first_document_start()
        self._first_document = False
        return ret

    # pylint: disable=signature-differs
    def write_indicator(self, indicator, *args, **kwargs):
        # NOTE: if the yaml file already have a directive,
        #  this will strip it
        if isinstance(self.event, DocumentStartEvent):
            skip_marker = (
                # see comments in _expect_first_document_start()
                getattr(self, "_first_document", False)
                and not self.event.explicit
                and not self.canonical
                and not self.event.tags
            )
            if skip_marker and indicator == self.MARKER_START_LINE:
                return
        super().write_indicator(indicator, *args, **kwargs)


def _dump_yaml(data, stream, *, version=None, with_directive=False):
    yaml = YAML()
    if version in (None, YAMLVersion.V11):
        yaml.version = YAMLVersion.V11
        if not with_directive:
            yaml.Emitter = _YAMLEmitterNoVersionDirective
    elif with_directive and version == YAMLVersion.V12:
        # `ruamel.yaml` dumps in 1.2 by default
        yaml.version = version

    yaml.default_flow_style = False
    yaml.Representer.add_representer(
        OrderedDict, yaml.Representer.represent_dict
    )
    yaml.dump(data, stream)


def dump_yaml(path, data, *, version=None, with_directive=False):
    with open(path, "w", encoding="utf-8") as fd:
        _dump_yaml(data, fd, version=version, with_directive=with_directive)
