from __future__ import unicode_literals

import platform
import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.version import __version__


logger = logging.getLogger(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        info = (
            "DVC version: {dvc_version}\n"
            "Python version: {python_version}\n"
            "Platform: {platform_type}"
        ).format(
            dvc_version=__version__,
            python_version=platform.python_version(),
            platform_type=platform.platform(),
        )
        logger.info(info)
        return 0


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
