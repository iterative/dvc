from __future__ import unicode_literals

import os

from dvc.command.base import CmdBaseNoRepo
from dvc.command.base import fix_subparsers


class CmdDaemonBase(CmdBaseNoRepo):
    pass


class CmdDaemonUpdater(CmdDaemonBase):
    def run(self):
        from dvc.repo import Repo
        from dvc.updater import Updater

        root_dir = Repo.find_root()
        dvc_dir = os.path.join(root_dir, Repo.DVC_DIR)
        updater = Updater(dvc_dir)
        updater.fetch(detach=False)

        return 0


class CmdDaemonAnalytics(CmdDaemonBase):
    def run(self):
        """Collect and send analytics"""
        from dvc import analytics

        report = analytics.collect(self.args.cmd_class, self.args.ret)
        analytics.send(report)

        return 0


def add_parser(subparsers, parent_parser):
    DAEMON_HELP = "Service daemon."
    daemon_parser = subparsers.add_parser(
        "daemon",
        parents=[parent_parser],
        description=DAEMON_HELP,
        add_help=False,
    )

    daemon_subparsers = daemon_parser.add_subparsers(
        dest="cmd", help="Use dvc daemon CMD --help for command-specific help."
    )

    fix_subparsers(daemon_subparsers)

    DAEMON_UPDATER_HELP = "Fetch latest available version."
    daemon_updater_parser = daemon_subparsers.add_parser(
        "updater",
        parents=[parent_parser],
        description=DAEMON_UPDATER_HELP,
        help=DAEMON_UPDATER_HELP,
    )
    daemon_updater_parser.set_defaults(func=CmdDaemonUpdater)

    DAEMON_ANALYTICS_HELP = "Collect and send dvc usage analytics."
    daemon_analytics_parser = daemon_subparsers.add_parser(
        "analytics",
        parents=[parent_parser],
        description=DAEMON_ANALYTICS_HELP,
        help=DAEMON_ANALYTICS_HELP,
    )
    daemon_analytics_parser.add_argument(
        "cmd_class", help="Class called through main"
    )
    daemon_analytics_parser.add_argument(
        "ret", help="Return code from running such class"
    )
    daemon_analytics_parser.set_defaults(func=CmdDaemonAnalytics)
