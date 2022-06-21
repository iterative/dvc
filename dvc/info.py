import importlib.metadata as importlib_metadata
import itertools
import os
import pathlib
import platform

import psutil

from dvc import __version__
from dvc.exceptions import NotDvcRepoError
from dvc.fs import FS_MAP, generic, get_fs_cls, get_fs_config
from dvc.repo import Repo
from dvc.scm import SCMError
from dvc.utils import error_link
from dvc.utils.pkg import PKG

package = "" if PKG is None else f"({PKG})"


def get_dvc_info():
    info = [
        f"DVC version: {__version__} {package}",
        "---------------------------------",
        f"Platform: Python {platform.python_version()} on "
        f"{platform.platform()}",
        f"Supports:{_get_supported_remotes()}",
    ]

    try:
        with Repo() as repo:
            # cache_dir might not exist yet (e.g. after `dvc init`), and we
            # can't auto-create it, as it might cause issues if the user
            # later decides to enable shared cache mode with
            # `dvc config cache.shared group`.
            if os.path.exists(repo.odb.local.cache_dir):
                info.append(f"Cache types: {_get_linktype_support_info(repo)}")
                fs_type = get_fs_type(repo.odb.local.cache_dir)
                info.append(f"Cache directory: {fs_type}")
            else:
                info.append("Cache types: " + error_link("no-dvc-cache"))

            info.append(f"Caches: {_get_caches(repo.odb)}")
            info.append(f"Remotes: {_get_remotes(repo.config)}")

            root_directory = repo.root_dir
            fs_root = get_fs_type(os.path.abspath(root_directory))
            info.append(f"Workspace directory: {fs_root}")
            info.append(f"Repo: {_get_dvc_repo_info(repo)}")
    except NotDvcRepoError:
        pass
    except SCMError:
        info.append("Repo: dvc, git (broken)")

    return "\n".join(info)


def _get_caches(cache):
    caches = (
        cache_type
        for cache_type, cache_instance in cache.by_scheme()
        if cache_instance
    )

    # Caches will be always non-empty including the local cache
    return ", ".join(caches)


def _get_remotes(config):
    schemes = (
        get_fs_cls(get_fs_config(None, config, name=remote)).protocol
        for remote in config["remote"]
    )

    return ", ".join(schemes) or "None"


def _get_linktype_support_info(repo):
    odb = repo.odb.local

    links = generic.test_links(
        ["reflink", "hardlink", "symlink"],
        odb.fs,
        odb.path,
        repo.fs,
        repo.root_dir,
    )

    return ", ".join(links)


def _get_supported_remotes():
    supported_remotes = []
    for scheme, fs_cls in FS_MAP.items():
        if not fs_cls.get_missing_deps():
            dependencies = []
            for requirement in fs_cls.REQUIRES:
                dependencies.append(
                    f"{requirement} = "
                    f"{importlib_metadata.version(requirement)}"
                )

            remote_info = scheme
            if dependencies:
                remote_info += " (" + ", ".join(dependencies) + ")"
            supported_remotes.append(remote_info)

    assert len(supported_remotes) >= 1
    return "\n\t" + ",\n\t".join(supported_remotes)


def get_fs_type(path):
    partition = {}
    for part in psutil.disk_partitions(all=True):
        if part.fstype != "":
            try:
                mountpoint = pathlib.Path(part.mountpoint).resolve()
                partition[mountpoint] = part.fstype + " on " + part.device
            except PermissionError:
                pass

    # need to follow the symlink: https://github.com/iterative/dvc/issues/5065
    path = pathlib.Path(path).resolve()

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
