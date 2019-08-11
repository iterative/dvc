from __future__ import unicode_literals

import os
import platform
import argparse
import logging
import uuid

try:
    import psutil
except ImportError:
    psutil = None

from dvc.utils import is_binary
from dvc.utils.compat import pathlib
from dvc.repo import Repo
from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.version import __version__
from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.system import System


logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        dvc_version = __version__
        python_version = platform.python_version()
        platform_type = platform.platform()
        binary = is_binary()

        info = (
            "DVC version: {dvc_version}\n"
            "Python version: {python_version}\n"
            "Platform: {platform_type}\n"
            "Binary: {binary}\n"
        ).format(
            dvc_version=dvc_version,
            python_version=python_version,
            platform_type=platform_type,
            binary=binary,
        )

        try:
            repo = Repo()
            root_directory = repo.root_dir

            info += "Cache: {cache}\n".format(
                cache=self.get_linktype_support_info(repo)
            )

            if psutil:
                info += (
                    "Filesystem type (cache directory): {fs_cache}\n"
                ).format(fs_cache=self.get_fs_type(repo.cache.local.cache_dir))
        except NotDvcRepoError:
            root_directory = os.getcwd()

        if psutil:
            info += ("Filesystem type (workspace): {fs_root}").format(
                fs_root=self.get_fs_type(os.path.abspath(root_directory))
            )

        logger.info(info)
        return 0

    @staticmethod
    def get_fs_type(path):
        partition = {
            pathlib.Path(part.mountpoint): (part.fstype, part.device)
            for part in psutil.disk_partitions()
        }
        for parent in pathlib.Path(path).parents:
            if parent in partition:
                return partition[parent]
        return ("unkown", "none")

    @staticmethod
    def get_linktype_support_info(repo):
        links = {
            "reflink": System.reflink,
            "hardlink": System.hardlink,
            "symlink": System.symlink,
        }

        fname = "." + str(uuid.uuid4())
        src = os.path.join(repo.cache.local.cache_dir, fname)
        open(src, "w").close()
        dst = os.path.join(repo.root_dir, fname)

        cache = []

        for name, link in links.items():
            try:
                link(src, dst)
                os.unlink(dst)
                supported = True
            except DvcException:
                supported = False
            cache.append(
                "{name} - {supported}".format(
                    name=name, supported=True if supported else False
                )
            )
        os.remove(src)

        return ", ".join(cache)


def add_parser(subparsers, parent_parser):
    VERSION_HELP = "Show DVC version and system/environment informaion."

    version_parser = subparsers.add_parser(
        "version",
        parents=[parent_parser],
        description=append_doc_link(VERSION_HELP, "version"),
        help=VERSION_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    version_parser.set_defaults(func=CmdVersion)
