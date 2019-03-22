from __future__ import unicode_literals

import dvc.prompt as prompt
import dvc.logger as logger
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


class CmdDestroy(CmdBase):
    def run_cmd(self):
        try:
            statement = (
                "This will destroy all information about your pipelines,"
                " all data files, as well as cache in .dvc/cache."
                "\n"
                "Are you sure you want to continue?"
            )

            if not self.args.force and not prompt.confirm(statement):
                raise DvcException(
                    "cannot destroy without a confirmation from the user."
                    " Use '-f' to force."
                )

            self.repo.destroy()
        except Exception:
            logger.error("failed to destroy DVC")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    DESTROY_HELP = (
        "Destroy dvc. Will remove all repo's information, "
        "data files and cache.\n"
        "documentation: https://man.dvc.org/destroy"
    )
    destroy_parser = subparsers.add_parser(
        "destroy",
        parents=[parent_parser],
        description=DESTROY_HELP,
        help=DESTROY_HELP,
    )
    destroy_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force destruction.",
    )
    destroy_parser.set_defaults(func=CmdDestroy)
