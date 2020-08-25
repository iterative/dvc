import io
from collections import OrderedDict
from contextlib import contextmanager

from funcy import reraise
from jinja2 import Template
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ._common import ParseError, _dump_data, _load_data, _modify_data


class YAMLFileCorruptedError(ParseError):
    def __init__(self, path):
        super().__init__(path, "YAML file structure is corrupted")


def recursive_render(tpl, values, max_passes=100):
    """This is a bit of black magic to recursivly render a
    template. Adaped from:

      https://stackoverflow.com/questions/8862731/jinja-nested-rendering-on-variable-content

    Args:
      tpl: Template string
      values: dict of values. Importantly this dict can contain
          values that are themselves {{ placeholders }}
      max_passes: Limits the number of times we loop over the
          template.

    Returns:
      rendered template.
    """
    prev = tpl
    for _ in range(max_passes):
        curr = Template(prev).render(**values)
        if curr != prev:
            prev = curr
        else:
            return curr
    raise RecursionError("Max resursion depth reached")


def render_vars(dvc_dict):
    vars_dict = dvc_dict["vars"]
    vars_template = dumps_yaml(vars_dict)

    rendered_vars = recursive_render(vars_template, vars_dict)
    return rendered_vars


def render_dvc(dvc_dict, vars_dict):
    dvc_template = dumps_yaml(dvc_dict)
    rendered_dvc = Template(dvc_template).render(**vars_dict)
    return rendered_dvc


def render_dvc_template(text):

    yaml = YAML(typ="safe")
    dvc_dict = yaml.load(text) or {}
    if "vars" in dvc_dict:
        vars_dict = yaml.load(render_vars(dvc_dict))

        del dvc_dict["vars"]
        rendered_dvc = yaml.load(render_dvc(dvc_dict, vars_dict)) or {}

    return rendered_dvc, vars_dict


def parse_yaml(text, path, typ="safe"):
    yaml = YAML(typ=typ)
    with reraise(YAMLError, YAMLFileCorruptedError(path)):
        result = yaml.load(text) or {}

    if "vars" in result:
        try:
            result, _ = render_dvc_template(
                text
            )  # yaml.load(text, Loader=SafeLoader) or {}
        except Exception as exc:
            raise YAMLFileCorruptedError(path) from exc

    return result


def load_yaml(path, tree=None):
    return _load_data(path, parser=parse_yaml, tree=tree)


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


def _dump(data, stream):
    yaml = _get_yaml()
    return yaml.dump(data, stream)


def dump_yaml(path, data, tree=None):
    return _dump_data(path, data, dumper=_dump, tree=tree)


def loads_yaml(s, typ="safe"):
    return YAML(typ=typ).load(s)


def dumps_yaml(d):
    stream = io.StringIO()
    _dump(d, stream)
    return stream.getvalue()


@contextmanager
def modify_yaml(path, tree=None):
    with _modify_data(path, parse_yaml_for_update, dump_yaml, tree=tree) as d:
        yield d
