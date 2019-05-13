from __future__ import unicode_literals

import os
import psutil
import platform
import argparse
import logging

from dvc.repo import Repo
from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.version import __version__
from dvc.exceptions import NotDvcRepoError


logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        try:
            root_directory = Repo.find_root()

            info = (
                "DVC version: {dvc_version}\n"
                "Python version: {python_version}\n"
                "Platform: {platform_type}\n"
                "Filesystem Type: {filesystem_type}\n"
                "Symlink: {symlink}"
            ).format(
                dvc_version=__version__,
                python_version=platform.python_version(),
                platform_type=platform.platform(),
                filesystem_type=self.get_fs_type(
                    os.path.abspath(root_directory)
                ),
                symlink=os.path.islink(os.path.abspath(root_directory)),
            )
            logger.info(info)
            return 0

        except NotDvcRepoError:
            logger.info("Not inside a dvc directory.")
            return 1

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
