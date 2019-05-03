from __future__ import unicode_literals

import platform
import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.version import __version__


logger = logging.getLogger(__name__)


class CmdVersion(CmdBase):
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
    ADD_HELP = (
        "Output the system/environment information along with the DVC version"
    )

    add_parser = subparsers.add_parser(
        "version",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "version"),
        help=ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_parser.set_defaults(func=CmdVersion)
