from __future__ import unicode_literals

import os
import psutil
import platform
import argparse
import logging
import uuid

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

        info = (
            "DVC version: {dvc_version}\n"
            "Python version: {python_version}\n"
            "Platform: {platform_type}\n"
        ).format(
            dvc_version=dvc_version,
            python_version=python_version,
            platform_type=platform_type,
        )

        try:
            root_directory = Repo.find_root()

            info += (
                "Filesystem type: {filesystem_type}\n" "Cache: {cache}"
            ).format(
                filesystem_type=self.get_fs_type(
                    os.path.abspath(root_directory)
                ),
                cache=self.get_linktype_support_info(),
            )

        except NotDvcRepoError:
            root_directory = os.getcwd()

            info += ("Filesystem type: {filesystem_type}").format(
                filesystem_type=self.get_fs_type(
                    os.path.abspath(root_directory)
                )
            )

        logger.info(info)
        return 0

    def get_fs_type(self, path):
        partition = {}
        for part in psutil.disk_partitions():
            partition[part.mountpoint] = (part.fstype, part.device)
        if path in partition:
            return partition[path]
        splitpath = path.split(os.sep)
        for i in range(len(splitpath), 0, -1):
            path = os.sep.join(splitpath[:i]) + os.sep
            if path in partition:
                return partition[path]
            path = os.sep.join(splitpath[:i])
            if path in partition:
                return partition[path]
        return ("unkown", "none")

    def get_linktype_support_info(self):
        links = {
            "reflink": System.reflink,
            "hardlink": System.hardlink,
            "symlink": System.symlink,
        }

        repo = Repo()
        fname = "." + str(uuid.uuid4())
        src = os.path.join(repo.cache.local.cache_dir, fname)
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
