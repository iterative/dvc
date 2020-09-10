import itertools
import os
import pathlib
import platform
import uuid

from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.repo import Repo
from dvc.scm.base import SCMError
from dvc.system import System
from dvc.tree import TREES
from dvc.utils import error_link
from dvc.utils.pkg import PKG
from dvc.version import __version__

try:
    import psutil
except ImportError:
    psutil = None

if PKG is None:
    package = ""
else:
    package = f"({PKG})"


def get_dvc_info():
    info = [
        f"DVC version: {__version__} {package}",
        "---------------------------------",
        f"Platform: Python {platform.python_version()} on "
        f"{platform.platform()}",
        f"Supports: {_get_supported_remotes()}",
    ]

    try:
        repo = Repo()

        # cache_dir might not exist yet (e.g. after `dvc init`), and we
        # can't auto-create it, as it might cause issues if the user
        # later decides to enable shared cache mode with
        # `dvc config cache.shared group`.
        if os.path.exists(repo.cache.local.cache_dir):
            info.append(
                "Cache types: {}".format(_get_linktype_support_info(repo))
            )
            if psutil:
                fs_type = get_fs_type(repo.cache.local.cache_dir)
                info.append(f"Cache directory: {fs_type}")
        else:
            info.append("Cache types: " + error_link("no-dvc-cache"))

    except NotDvcRepoError:
        pass
    except SCMError:
        info.append("Repo: dvc, git (broken)")
    else:
        root_directory = repo.root_dir
        if psutil:
            fs_root = get_fs_type(os.path.abspath(root_directory))
            info.append(f"Workspace directory: {fs_root}")
        info.append("Repo: {}".format(_get_dvc_repo_info(repo)))
    return "\n".join(info)


def _get_linktype_support_info(repo):

    links = {
        "reflink": (System.reflink, None),
        "hardlink": (System.hardlink, System.is_hardlink),
        "symlink": (System.symlink, System.is_symlink),
    }

    fname = "." + str(uuid.uuid4())
    src = os.path.join(repo.cache.local.cache_dir, fname)
    open(src, "w").close()
    dst = os.path.join(repo.root_dir, fname)

    cache = []

    for name, (link, is_link) in links.items():
        try:
            link(src, dst)
            status = "supported"
            if is_link and not is_link(dst):
                status = "broken"
            os.unlink(dst)
        except DvcException:
            status = "not supported"

        if status == "supported":
            cache.append(name)
    os.remove(src)

    return ", ".join(cache)


def _get_supported_remotes():

    supported_remotes = []
    for tree_cls in TREES:
        if not tree_cls.get_missing_deps():
            supported_remotes.append(tree_cls.scheme)

    if len(supported_remotes) == len(TREES):
        return "All remotes"

    if len(supported_remotes) == 1:
        return supported_remotes

    return ", ".join(supported_remotes)


def get_fs_type(path):

    partition = {
        pathlib.Path(part.mountpoint): (part.fstype + " on " + part.device)
        for part in psutil.disk_partitions(all=True)
    }

    path = pathlib.Path(path)

    for parent in itertools.chain([path], path.parents):
        if parent in partition:
            return partition[parent]
    return ("unknown", "none")


def _get_dvc_repo_info(self):
    if self.config.get("core", {}).get("no_scm", False):
        return "dvc (no_scm)"

    if self.root_dir != self.scm.root_dir:
        return "dvc (subdir), git"

    return "dvc, git"
