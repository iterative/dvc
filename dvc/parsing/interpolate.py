import re
from collections.abc import Mapping

from funcy import lmap, rpartial

from dvc.parsing.context import Value

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


def _find_match(template):
    return list(KEYCRE.finditer(template))


def _str_interpolate(template, replace_strings):
    buf = str(template)
    for replace_string, value in replace_strings.items():
        if not isinstance(value, Value):
            raise TypeError(
                "Cannot interpolate to string that's not a primitive",
                "received: ",
                type(value),
            )
        buf = buf.replace(replace_string, str(value))
    return buf


def _resolve_str(src, context):
    if isinstance(src, str):
        matches = _find_match(src)
        num_matches = len(matches)
        if num_matches:
            to_replace = {}
            for match in matches:
                expr = match.group()
                expand_and_track, _, inner = match.groups()
                track = expand_and_track
                if expr not in to_replace:
                    to_replace[expr] = context.select(inner, track=track)
            # replace "${enabled}", if `enabled` is a boolean, with it's actual
            # value rather than it's string counterparts.
            if num_matches == 1 and src == matches[0].group(0):
                values = list(to_replace.values())
                assert len(values) == 1
                return values[0].value
            # but not "${num} days"
            src = _str_interpolate(src, to_replace)

    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\`). We just need to replace those by `${`.
    # FIXME: how to fix "\${abc} ${abc}"? right now, both will be replaced
    return src.replace(r"\${", "${")


def resolve(src, context):
    # TODO: can we do this better?
    Seq = (list, tuple, set)

    apply_value = rpartial(resolve, context)
    if isinstance(src, Mapping):
        return {key: apply_value(value) for key, value in src.items()}
    elif isinstance(src, Seq):
        return lmap(apply_value, src)
    return _resolve_str(src, context)
