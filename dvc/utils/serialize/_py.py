import ast

from funcy import reraise

from ._common import ParseError, _load_data


class PythonFileCorruptedError(ParseError):
    def __init__(self, path, message="PY file structure is corrupted"):
        super().__init__(path, message)


def load_py(path, tree=None):
    return _load_data(path, parser=parse_py, tree=tree)


def parse_py(text, path):
    """Parses text from .py file into Python structure."""
    with reraise(SyntaxError, PythonFileCorruptedError(path)):
        tree = ast.parse(text, filename=path)

    result = _ast_tree_to_dict(tree)
    return result


def _ast_tree_to_dict(tree, only_self_params=False):
    """Parses ast trees to dict."""
    result = {}
    for _body in tree.body:
        try:
            if isinstance(_body, (ast.Assign, ast.AnnAssign)):
                result.update(_ast_assign_to_dict(_body, only_self_params))
            elif isinstance(_body, ast.ClassDef):
                result.update({_body.name: _ast_tree_to_dict(_body)})
            elif (
                isinstance(_body, ast.FunctionDef) and _body.name == "__init__"
            ):
                result.update(_ast_tree_to_dict(_body, only_self_params=True))
        except ValueError:
            continue
        except AttributeError:
            continue
    return result


def _ast_assign_to_dict(assign, only_self_params=False):
    result = {}

    if isinstance(assign, ast.AnnAssign):
        name = _get_ast_name(assign.target, only_self_params)
    elif len(assign.targets) == 1:
        name = _get_ast_name(assign.targets[0], only_self_params)
    else:
        raise AttributeError

    if isinstance(assign.value, ast.Dict):
        _dct = {}
        for key, val in zip(assign.value.keys, assign.value.values):
            _dct[_get_ast_value(key)] = _get_ast_value(val)
        result[name] = _dct
    elif isinstance(assign.value, ast.List):
        result[name] = [_get_ast_value(val) for val in assign.value.elts]
    elif isinstance(assign.value, ast.Set):
        values = [_get_ast_value(val) for val in assign.value.elts]
        result[name] = set(values)
    elif isinstance(assign.value, ast.Tuple):
        values = [_get_ast_value(val) for val in assign.value.elts]
        result[name] = tuple(values)
    else:
        result[name] = _get_ast_value(assign.value)

    return result


def _get_ast_name(target, only_self_params=False):
    if hasattr(target, "id") and not only_self_params:
        result = target.id
    elif hasattr(target, "attr") and target.value.id == "self":
        result = target.attr
    else:
        raise AttributeError
    return result


def _get_ast_value(value):
    if isinstance(value, ast.Num):
        result = value.n
    elif isinstance(value, ast.Str):
        result = value.s
    elif isinstance(value, ast.NameConstant):
        result = value.value
    else:
        raise ValueError
    return result
