import re

KEYCRE = re.compile(
    r"""
    (?<!\\)                   # escape \${} or ${{}}
    \$                        # starts with $
    (?:({{)|({))              # either starts with double braces or single
    ([\w._ \\/-]*?)           # match every char, attr access through "."
    (?(1)}})(?(2)})           # end with same kinds of braces it opened with
""",
    re.VERBOSE,
)


def _maybe_int(s):
    try:
        return int(s)
    except ValueError:
        return s


def _get_item(val, attr):
    try:
        return val[attr]
    except KeyError:
        # for dict, index could be str or int, from `maybe_int()`
        # parsing above: i.e. {"2": "two"} vs {2: "two"}
        if not (isinstance(val, dict) and isinstance(attr, int)):
            raise
        return val[str(attr)]


def get_value(val, key):
    obj_and_attrs = map(_maybe_int, key.strip().split("."))
    value = val
    for attr in obj_and_attrs:
        if attr == "":
            raise ValueError("Syntax error!")

        try:
            value = _get_item(value, attr)
        except KeyError:
            msg = (
                f"Could not find '{attr}' "
                "while substituting "
                f"'{key}'.\n"
                f"Interpolating with: {val}"
            )
            raise ValueError(msg)
    return value


def find_match(template):
    return list(KEYCRE.finditer(template))


def str_interpolate(template, replace_strings):
    buf = str(template)
    for replace_string, value in replace_strings.items():
        buf = buf.replace(replace_string, str(value))
    return buf
