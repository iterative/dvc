"""Helpers for other modules."""

import hashlib
import json
import os
import re
import sys
from typing import TYPE_CHECKING, Optional

import colorama

if TYPE_CHECKING:
    from typing import TextIO


LARGE_DIR_SIZE = 100
TARGET_REGEX = re.compile(r"(?P<path>.*?)(:(?P<name>[^\\/:]*))??$")


def bytes_hash(byts, typ):
    hasher = getattr(hashlib, typ)(usedforsecurity=False)
    hasher.update(byts)
    return hasher.hexdigest()


def dict_filter(d, exclude=()):
    """
    Exclude specified keys from a nested dict
    """
    if not exclude or not isinstance(d, (list, dict)):
        return d

    if isinstance(d, list):
        return [dict_filter(e, exclude) for e in d]

    return {k: dict_filter(v, exclude) for k, v in d.items() if k not in exclude}


def dict_hash(d, typ, exclude=()):
    filtered = dict_filter(d, exclude)
    byts = json.dumps(filtered, sort_keys=True).encode("utf-8")
    return bytes_hash(byts, typ)


def dict_md5(d, **kwargs):
    return dict_hash(d, "md5", **kwargs)


def dict_sha256(d, **kwargs):
    return dict_hash(d, "sha256", **kwargs)


def _split(list_to_split, chunk_size):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


# NOTE: Check if we are in a bundle
# https://pythonhosted.org/PyInstaller/runtime-information.html
def is_binary():
    return getattr(sys, "frozen", False)


def fix_env(env=None):
    """Fix env variables modified by PyInstaller [1] and pyenv [2].
    [1] http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
    [2] https://github.com/pyenv/pyenv/issues/985
    """
    if env is None:
        env = os.environ.copy()
    else:
        env = env.copy()

    if is_binary():
        lp_key = "LD_LIBRARY_PATH"
        lp_orig = env.get(lp_key + "_ORIG", None)
        if lp_orig is not None:
            env[lp_key] = lp_orig
        else:
            env.pop(lp_key, None)

    # Unlike PyInstaller, pyenv doesn't leave backups of original env vars
    # when it modifies them. If we look into the shim, pyenv and pyenv-exec,
    # we can figure out that the PATH is modified like this:
    #
    #     PATH=$PYENV_BIN_PATH:${bin_path}:${plugin_bin}:$PATH
    #
    # where
    #
    #     PYENV_BIN_PATH - might not start with $PYENV_ROOT if we are running
    #         `system` version of the command, see pyenv-exec source code.
    #     bin_path - might not start with $PYENV_ROOT as it runs realpath on
    #         it, but always has `libexec` part in it, see pyenv source code.
    #     plugin_bin - might contain more than 1 entry, which start with
    #         $PYENV_ROOT, see pyenv source code.
    #
    # Also, we know that whenever pyenv is running, it exports these env vars:
    #
    #     PYENV_DIR
    #     PYENV_HOOK_PATH
    #     PYENV_VERSION
    #     PYENV_ROOT
    #
    # So having this, we can make a rightful assumption about what parts of the
    # PATH we need to remove in order to get the original PATH.
    path = env.get("PATH", "")
    parts = path.split(":")
    bin_path = parts[1] if len(parts) > 2 else ""
    pyenv_dir = env.get("PYENV_DIR")
    pyenv_hook_path = env.get("PYENV_HOOK_PATH")
    pyenv_version = env.get("PYENV_VERSION")
    pyenv_root = env.get("PYENV_ROOT")

    env_matches = all([pyenv_dir, pyenv_hook_path, pyenv_version, pyenv_root])

    bin_path_matches = os.path.basename(bin_path) == "libexec"

    # NOTE: we don't support pyenv-win
    if os.name != "nt" and env_matches and bin_path_matches:
        # removing PYENV_BIN_PATH and bin_path
        parts = parts[2:]

        if parts:
            # removing plugin_bin from the left
            plugin_bin = os.path.join(pyenv_root, "plugins")
            while parts[0].startswith(plugin_bin):
                del parts[0]

        env["PATH"] = ":".join(parts)

    return env


def colorize(message, color=None, style=None):
    """Returns a message in a specified color."""
    if not color:
        return message

    styles = {"dim": colorama.Style.DIM, "bold": colorama.Style.BRIGHT}

    colors = {
        "green": colorama.Fore.GREEN,
        "yellow": colorama.Fore.YELLOW,
        "blue": colorama.Fore.BLUE,
        "red": colorama.Fore.RED,
        "magenta": colorama.Fore.MAGENTA,
        "cyan": colorama.Fore.CYAN,
    }

    return "{style}{color}{message}{reset}".format(
        style=styles.get(style, ""),
        color=colors.get(color, ""),
        message=message,
        reset=colorama.Style.RESET_ALL,
    )


def boxify(message, border_color=None):
    """Put a message inside a box.

    Args:
        message (unicode): message to decorate.
        border_color (unicode): name of the color to outline the box with.
    """
    lines = message.split("\n")
    max_width = max(_visual_width(line) for line in lines)

    padding_horizontal = 5
    padding_vertical = 1

    box_size_horizontal = max_width + (padding_horizontal * 2)

    chars = {"corner": "+", "horizontal": "-", "vertical": "|", "empty": " "}

    margin = "{corner}{line}{corner}\n".format(
        corner=chars["corner"], line=chars["horizontal"] * box_size_horizontal
    )

    padding_lines = [
        "{border}{space}{border}\n".format(
            border=colorize(chars["vertical"], color=border_color),
            space=chars["empty"] * box_size_horizontal,
        )
        * padding_vertical
    ]

    content_lines = [
        "{border}{space}{content}{space}{border}\n".format(
            border=colorize(chars["vertical"], color=border_color),
            space=chars["empty"] * padding_horizontal,
            content=_visual_center(line, max_width),
        )
        for line in lines
    ]

    return "{margin}{padding}{content}{padding}{margin}".format(
        margin=colorize(margin, color=border_color),
        padding="".join(padding_lines),
        content="".join(content_lines),
    )


def _visual_width(line):
    """Get the number of columns required to display a string"""

    return len(re.sub(colorama.ansitowin32.AnsiToWin32.ANSI_CSI_RE, "", line))


def _visual_center(line, width):
    """Center align string according to it's visual width"""

    spaces = max(width - _visual_width(line), 0)
    left_padding = int(spaces / 2)
    right_padding = spaces - left_padding

    return (left_padding * " ") + line + (right_padding * " ")


def relpath(path, start=os.curdir):
    path = os.path.abspath(os.fspath(path))
    start = os.path.abspath(os.fspath(start))

    # Windows path on different drive than curdir doesn't have relpath
    if os.name == "nt" and not os.path.commonprefix([start, path]):
        return path

    return os.path.relpath(path, start)


def as_posix(path: str) -> str:
    import ntpath
    import posixpath

    return path.replace(ntpath.sep, posixpath.sep)


def env2bool(var, undefined=False):
    """
    undefined: return value if env var is unset
    """
    var = os.getenv(var, None)
    if var is None:
        return undefined
    return bool(re.search("1|y|yes|true", var, flags=re.IGNORECASE))


def resolve_output(inp: str, out: Optional[str], force=False) -> str:
    from urllib.parse import urlparse

    from dvc.exceptions import FileExistsLocallyError

    name = os.path.basename(os.path.normpath(urlparse(inp).path))
    if not out:
        ret = name
    elif os.path.isdir(out):
        ret = os.path.join(out, name)
    else:
        ret = out

    if os.path.exists(ret) and not force:
        hint = "\nTo override it, re-run with '--force'."
        raise FileExistsLocallyError(ret, hint=hint)

    return ret


def resolve_paths(repo, out, always_local=False):
    from urllib.parse import urlparse

    from dvc.dvcfile import DVC_FILE_SUFFIX
    from dvc.exceptions import DvcException
    from dvc.fs import localfs

    from .fs import contains_symlink_up_to

    abspath = os.path.abspath(out)
    dirname = os.path.dirname(abspath)
    base = os.path.basename(os.path.normpath(out))

    scheme = urlparse(out).scheme

    if os.name == "nt" and scheme == os.path.splitdrive(abspath)[0][0].lower():
        # urlparse interprets windows drive letters as URL scheme
        scheme = ""

    if scheme or not localfs.isin_or_eq(abspath, repo.root_dir):
        wdir = os.getcwd()
    elif contains_symlink_up_to(dirname, repo.root_dir) or (
        os.path.isdir(abspath) and localfs.is_symlink(abspath)
    ):
        msg = (
            "Cannot add files inside symlinked directories to DVC. "
            "See {} for more information."
        ).format(
            format_link("https://dvc.org/doc/user-guide/troubleshooting#add-symlink")
        )
        raise DvcException(msg)
    else:
        wdir = dirname
        out = base

    if always_local:
        out = base

    path = os.path.join(wdir, base + DVC_FILE_SUFFIX)

    return (path, wdir, out)


def format_link(link):
    return "<{blue}{link}{nc}>".format(  # noqa: UP032
        blue=colorama.Fore.CYAN, link=link, nc=colorama.Fore.RESET
    )


def error_link(name):
    return format_link(f"https://error.dvc.org/{name}")


def parse_target(
    target: str, default: Optional[str] = None, isa_glob: bool = False
) -> tuple[Optional[str], Optional[str]]:
    from dvc.dvcfile import LOCK_FILE, PROJECT_FILE, is_valid_filename
    from dvc.exceptions import DvcException
    from dvc.parsing import JOIN

    if not target:
        return None, None

    default = default or PROJECT_FILE
    if isa_glob:
        path, _, glob = target.rpartition(":")
        return path or default, glob or None

    # look for first "@", so as not to assume too much about stage name
    # eg: it might contain ":" in a generated stages from dict which might
    # affect further parsing with the regex.
    group, _, key = target.partition(JOIN)
    match = TARGET_REGEX.match(group)

    if not match:
        return target, None

    path, name = (match.group("path"), match.group("name"))

    if name and key:
        name += f"{JOIN}{key}"

    if path:
        if os.path.basename(path) == LOCK_FILE:
            raise DvcException(
                "Did you mean: `{}`?".format(target.replace(".lock", ".yaml", 1))
            )
        if not name:
            ret = (target, None)
            return ret if is_valid_filename(target) else ret[::-1]
    return path or default, name


def glob_targets(targets, glob=True, recursive=True):
    from dvc.exceptions import DvcException

    if not glob:
        return targets

    from glob import iglob

    results = [
        exp_target
        for target in targets
        for exp_target in iglob(target, recursive=recursive)
    ]

    if not results:
        msg = f"Glob {targets} has no matches."
        raise DvcException(msg)

    return results


def error_handler(func):
    def wrapper(*args, **kwargs):
        onerror = kwargs.get("onerror")
        result = {}

        try:
            vals = func(*args, **kwargs)
            if vals:
                result["data"] = vals
        except Exception as e:  # noqa: BLE001
            if onerror is not None:
                onerror(result, e, **kwargs)
        return result

    return wrapper


def errored_revisions(rev_data: dict) -> list:
    from dvc.utils.collections import nested_contains

    result = []
    for revision, data in rev_data.items():
        if nested_contains(data, "error"):
            result.append(revision)
    return result


def isatty(stream: "Optional[TextIO]") -> bool:
    if stream is None:
        return False
    return stream.isatty()
