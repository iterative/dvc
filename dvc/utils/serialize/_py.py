import ast
import dataclasses
import logging
import sys
from contextlib import contextmanager
from functools import partial
from typing import Any, Optional

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data

_PARAMS_KEY = "__params_old_key_for_update__"
_PARAMS_TEXT_KEY = "__params_text_key_for_update__"

logger = logging.getLogger(__name__)


class PythonFileCorruptedError(ParseError):
    def __init__(self, path, message="Python file structure is corrupted"):
        super().__init__(path, message)


def load_py(path, fs=None):
    return _load_data(path, parser=parse_py, fs=fs)


def parse_py(text, path):
    """Parses text from .py file into Python structure."""
    with reraise(SyntaxError, PythonFileCorruptedError(path)):
        tree = ast.parse(text, filename=path)

    result = _ast_tree_to_dict(tree, text)
    return result


def parse_py_for_update(text, path):
    """Parses text into dict for update params."""
    with reraise(SyntaxError, PythonFileCorruptedError(path)):
        tree = ast.parse(text, filename=path)

    result = _ast_tree_to_dict(tree, text)
    result.update({_PARAMS_KEY: _ast_tree_to_dict(tree, text, lineno=True)})
    result.update({_PARAMS_TEXT_KEY: text})
    return result


def _dump(data, stream):

    old_params = data[_PARAMS_KEY]
    new_params = {
        key: value
        for key, value in data.items()
        if key not in [_PARAMS_KEY, _PARAMS_TEXT_KEY]
    }
    old_lines = data[_PARAMS_TEXT_KEY].splitlines(True)

    def _update_lines(lines, old_dct, new_dct):
        if not isinstance(old_dct, dict):
            return lines

        for key, value in new_dct.items():
            old_value = old_dct.get(key)
            if isinstance(old_value, dict) and isinstance(value, dict):
                lines = _update_lines(lines, old_value, value)
                continue

            if isinstance(old_value, Node):
                if isinstance(value, dict):
                    logger.trace("Old %s is %s, new value is of type %s", key, old_value, type(value))
                    continue
            else:
                continue

            if old_value.value is not None and value == old_value.value:
                # we should try to reduce amount of updates
                # so if things didn't change at all or are equivalent
                # we don't need to dump at all.
                continue
            elif old_value.lineno is not None and (old_value.segment or old_value.value):
                old_segment = " = {}".format(old_value.segment or old_value.value)
                new_segment = " = {}".format(value)
                lineno = old_value.lineno
                logger.trace("updating lineno:", lineno)
                line = lines[lineno].replace(old_segment, new_segment)
                logger.trace("before: ", lines[lineno])
                lines[lineno] = line
                logger.trace("after: ", lines[lineno])

        return lines

    new_lines = _update_lines(old_lines, old_params, new_params)
    new_text = "".join(new_lines)

    try:
        ast.parse(new_text)
    except SyntaxError:
        raise PythonFileCorruptedError(
            stream.name,
            "Python file structure is corrupted after update params",
        )

    stream.write(new_text)
    stream.close()


def dump_py(path, data, fs=None):
    return _dump_data(path, data, dumper=_dump, fs=fs)


@contextmanager
def modify_py(path, fs=None):
    with _modify_data(path, parse_py_for_update, dump_py, fs=fs) as d:
        yield d


def _ast_tree_to_dict(tree, source, only_self_params=False, lineno=False):
    """Parses ast trees to dict.

    :param tree: ast.Tree
    :param only_self_params: get only self params from class __init__ function
    :param lineno: add params line number (needed for update)
    :return:
    """
    result = {}
    for _body in tree.body:
        try:
            if isinstance(_body, (ast.Assign, ast.AnnAssign)):
                result.update(
                    _ast_assign_to_dict(
                        _body, source, only_self_params, lineno
                    )
                )
            elif isinstance(_body, ast.ClassDef):
                result.update(
                    {
                        _body.name: _ast_tree_to_dict(
                            _body, source, lineno=lineno
                        )
                    }
                )
            elif (
                isinstance(_body, ast.FunctionDef) and _body.name == "__init__"
            ):
                result.update(
                    _ast_tree_to_dict(
                        _body, source, only_self_params=True, lineno=lineno
                    )
                )
        except ValueError:
            continue
        except AttributeError:
            continue
    return result


def _ast_assign_to_dict(assign, source, only_self_params=False, lineno=False):
    if isinstance(assign, ast.AnnAssign):
        name = _get_ast_name(assign.target, only_self_params)
    elif len(assign.targets) == 1:
        name = _get_ast_name(assign.targets[0], only_self_params)
    else:
        raise AttributeError
    return {name: _get_ast_value(assign.value, source, value_only=not lineno)}


def _get_ast_name(target, only_self_params=False):
    if hasattr(target, "id") and not only_self_params:
        result = target.id
    elif hasattr(target, "attr") and target.value.id == "self":
        result = target.attr
    else:
        raise AttributeError
    return result


def get_source_segment(source, node):
    if sys.version_info > (3, 8):
        return ast.get_source_segment(source, node)

    try:
        import astunparse

        return astunparse.unparse(node).rstrip()
    except:
        return None


@dataclasses.dataclass
class Node:
    value: Any
    lineno: Optional[int]
    segment: Optional[str]


def _get_ast_value(node, source=None, value_only: bool = False):
    from ast import literal_eval

    convert = partial(_get_ast_value, source=source, value_only=value_only)
    if isinstance(node, ast.Tuple):
        result = tuple(map(convert, node.elts))
    elif isinstance(node, ast.Set):
        result = set(map(convert, node.elts))
    elif isinstance(node, ast.Dict):
        result = dict(
            (_get_ast_value(k, value_only=True), convert(v))
            for k, v in zip(node.keys, node.values)
        )
    else:
        result = literal_eval(node)
        if value_only or not source:
            return result

        lno = node.lineno - 1
        segment = get_source_segment(source, node)
        return Node(result, lno, segment)

    return result
