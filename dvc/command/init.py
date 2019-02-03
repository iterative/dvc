from __future__ import unicode_literals

import dvc.logger as logger


class CmdInit(object):
    def __init__(self, args):
        self.args = args

    def run_cmd(self):
        from dvc.project import Project, InitError

        try:
            self.project = Project.init(
                ".", no_scm=self.args.no_scm, force=self.args.force
            )
            self.config = self.project.config
        except InitError:
            logger.error("failed to initiate dvc")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    """Setup parser for `dvc init`."""
    INIT_HELP = (
        "Initialize dvc over a directory " "(should already be a git dir)."
    )
    init_parser = subparsers.add_parser(
        "init", parents=[parent_parser], description=INIT_HELP, help=INIT_HELP
    )
    init_parser.add_argument(
        "--no-scm",
        action="store_true",
        default=False,
        help="Initiate dvc in directory that is "
        "not tracked by any scm tool (e.g. git).",
    )
    init_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite '.dvc' if it exists. Will remove all local cache.",
    )
    init_parser.set_defaults(func=CmdInit)
