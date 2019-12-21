"""Helpers for other modules."""
from __future__ import unicode_literals

import hashlib
import json
import logging
import math
import os
import re
import sys
import time

import colorama
import nanotime
from ruamel.yaml import YAML
from shortuuid import uuid

from dvc.utils.compat import builtin_str
from dvc.utils.compat import cast_bytes_py2
from dvc.utils.compat import fspath
from dvc.utils.compat import fspath_py35
from dvc.utils.compat import makedirs as _makedirs
from dvc.utils.compat import open
from dvc.utils.compat import str
from dvc.utils.compat import StringIO


logger = logging.getLogger(__name__)

LOCAL_CHUNK_SIZE = 2 ** 20  # 1 MB
LARGE_FILE_SIZE = 2 ** 30  # 1 GB
LARGE_DIR_SIZE = 100


def dos2unix(data):
    return data.replace(b"\r\n", b"\n")


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    from dvc.progress import Tqdm
    from dvc.istextfile import istextfile

    fname = fspath_py35(fname)

    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        binary = not istextfile(fname)
        size = os.path.getsize(fname)
        no_progress_bar = True
        if size >= LARGE_FILE_SIZE:
            no_progress_bar = False
            msg = (
                "Computing md5 for a large file '{}'. This is only done once."
            )
            logger.info(msg.format(relpath(fname)))
        name = relpath(fname)

        with Tqdm(
            desc=name,
            disable=no_progress_bar,
            total=size,
            bytes=True,
            leave=False,
        ) as pbar:
            with open(fname, "rb") as fobj:
                while True:
                    data = fobj.read(LOCAL_CHUNK_SIZE)
                    if not data:
                        break

                    if binary:
                        chunk = data
                    else:
                        chunk = dos2unix(data)

                    hash_md5.update(chunk)
                    pbar.update(len(data))

        return (hash_md5.hexdigest(), hash_md5.digest())

    return (None, None)


def bytes_md5(byts):
    hasher = hashlib.md5()
    hasher.update(byts)
    return hasher.hexdigest()


def dict_filter(d, exclude=()):
    """
    Exclude specified keys from a nested dict
    """

    def fix_key(k):
        return str(k) if isinstance(k, builtin_str) else k

    if isinstance(d, list):
        return [dict_filter(e, exclude) for e in d]

    if isinstance(d, dict):
        items = ((fix_key(k), v) for k, v in d.items())
        return {
            k: dict_filter(v, exclude) for k, v in items if k not in exclude
        }

    return d


def dict_md5(d, exclude=()):
    filtered = dict_filter(d, exclude)
    byts = json.dumps(filtered, sort_keys=True).encode("utf-8")
    return bytes_md5(byts)


def copyfile(src, dest, no_progress_bar=False, name=None):
    """Copy file with progress bar"""
    from dvc.exceptions import DvcException
    from dvc.progress import Tqdm
    from dvc.system import System

    src = fspath_py35(src)
    dest = fspath_py35(dest)

    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    try:
        System.reflink(src, dest)
    except DvcException:
        with Tqdm(
            desc=name, disable=no_progress_bar, total=total, bytes=True
        ) as pbar:
            with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
                while True:
                    buf = fsrc.read(LOCAL_CHUNK_SIZE)
                    if not buf:
                        break
                    fdest.write(buf)
                    pbar.update(len(buf))


def makedirs(path, exist_ok=False, mode=None):
    path = fspath_py35(path)

    if mode is None:
        _makedirs(path, exist_ok=exist_ok)
        return

    # utilize umask to set proper permissions since Python 3.7 the `mode`
    # `makedirs` argument no longer affects the file permission bits of
    # newly-created intermediate-level directories.
    umask = os.umask(0o777 - mode)
    try:
        _makedirs(path, exist_ok=exist_ok)
    finally:
        os.umask(umask)


def _split(list_to_split, chunk_size):
    return [
        list_to_split[i : i + chunk_size]
        for i in range(0, len(list_to_split), chunk_size)
    ]


def _to_chunks_by_chunks_number(list_to_split, num_chunks):
    chunk_size = int(math.ceil(float(len(list_to_split)) / num_chunks))

    if len(list_to_split) == 1:
        return [list_to_split]

    if chunk_size == 0:
        chunk_size = 1

    return _split(list_to_split, chunk_size)


def to_chunks(list_to_split, num_chunks=None, chunk_size=None):
    if (num_chunks and chunk_size) or (not num_chunks and not chunk_size):
        raise ValueError(
            "Only one among `num_chunks` or `chunk_size` must be defined."
        )
    if chunk_size:
        return _split(list_to_split, chunk_size)
    return _to_chunks_by_chunks_number(list_to_split, num_chunks)


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
            # NOTE: py2 doesn't like unicode strings in environ
            env[cast_bytes_py2(lp_key)] = cast_bytes_py2(lp_orig)
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


def tmp_fname(fname):
    """ Temporary name for a partial download """
    return fspath(fname) + "." + str(uuid()) + ".tmp"


def current_timestamp():
    return int(nanotime.timestamp(time.time()))


def from_yaml_string(s):
    return YAML().load(StringIO(s))


def to_yaml_string(data):
    stream = StringIO()
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.dump(data, stream)
    return stream.getvalue()


def dvc_walk(top, dvcignore, topdown=True, onerror=None, followlinks=False):
    """
    Proxy for `os.walk` directory tree generator.
    Utilizes DvcIgnoreFilter functionality.
    """
    top = fspath_py35(top)

    for root, dirs, files in os.walk(
        top, topdown=topdown, onerror=onerror, followlinks=followlinks
    ):

        if dvcignore:
            dirs[:], files[:] = dvcignore(root, dirs, files)

        yield root, dirs, files


def walk_files(directory, dvcignore):
    for root, _, files in dvc_walk(directory, dvcignore):
        for f in files:
            yield os.path.join(root, f)


def colorize(message, color=None):
    """Returns a message in a specified color."""
    if not color:
        return message

    colors = {
        "green": colorama.Fore.GREEN,
        "yellow": colorama.Fore.YELLOW,
        "blue": colorama.Fore.BLUE,
        "red": colorama.Fore.RED,
    }

    return "{color}{message}{nc}".format(
        color=colors.get(color, ""), message=message, nc=colorama.Fore.RESET
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

    box_str = "{margin}{padding}{content}{padding}{margin}".format(
        margin=colorize(margin, color=border_color),
        padding="".join(padding_lines),
        content="".join(content_lines),
    )

    return box_str


def _visual_width(line):
    """Get the the number of columns required to display a string"""

    return len(re.sub(colorama.ansitowin32.AnsiToWin32.ANSI_CSI_RE, "", line))


def _visual_center(line, width):
    """Center align string according to it's visual width"""

    spaces = max(width - _visual_width(line), 0)
    left_padding = int(spaces / 2)
    right_padding = spaces - left_padding

    return (left_padding * " ") + line + (right_padding * " ")


def relpath(path, start=os.curdir):
    path = fspath(path)
    start = os.path.abspath(fspath(start))

    # Windows path on different drive than curdir doesn't have relpath
    if os.name == "nt" and not os.path.commonprefix(
        [start, os.path.abspath(path)]
    ):
        return path
    return os.path.relpath(path, start)


def env2bool(var, undefined=False):
    """
    undefined: return value if env var is unset
    """
    var = os.getenv(var, None)
    if var is None:
        return undefined
    return bool(re.search("1|y|yes|true", var, flags=re.I))


def resolve_output(inp, out):
    from dvc.utils.compat import urlparse

    name = os.path.basename(urlparse(inp).path)
    if not out:
        return name
    if os.path.isdir(out):
        return os.path.join(out, name)
    return out


def format_link(link):
    import colorama

    return "<{blue}{link}{nc}>".format(
        blue=colorama.Fore.CYAN, link=link, nc=colorama.Fore.RESET
    )
