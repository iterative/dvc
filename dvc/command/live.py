import argparse
import logging
import os

from dvc.command import completion
from dvc.command.base import CmdBase, fix_subparsers
from dvc.utils.html import write

logger = logging.getLogger(__name__)


class CmdLive(CmdBase):
    UNINITIALIZED = True

    def _run(self, target, revs=None):
        metrics, plots = self.repo.live.show(target, revs)

        html_path = self.args.target + ".html"
        write(html_path, plots, metrics)

        logger.info(f"\nfile://{os.path.abspath(html_path)}")

        return 0


class CmdLiveShow(CmdLive):
    def run(self):
        self._run(self.args.target)


class CmdLiveDiff(CmdLive):
    def run(self):
        self._run(self.args.target, self.args.revs)


def add_parser(subparsers, parent_parser):
    LIVE_DESCRIPTION = (
        "Commands to visualize and compare dvclive-produced logs."
    )

    live_parser = subparsers.add_parser(
        "live",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=LIVE_DESCRIPTION,
    )
    live_subparsers = live_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc live CMD --help` to display command-specific help.",
    )

    fix_subparsers(live_subparsers)

    SHOW_HELP = "Visualize dvclive directory content."
    live_show_parser = live_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_common_arguments(live_show_parser)
    live_show_parser.set_defaults(func=CmdLiveShow)

    DIFF_HELP = (
        "Show multiple versions of dvclive data, "
        "by plotting it in single view."
    )
    live_diff_parser = live_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        help=DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    live_diff_parser.add_argument(
        "--revs",
        nargs="*",
        default=None,
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    _add_common_arguments(live_diff_parser)
    live_diff_parser.set_defaults(func=CmdLiveDiff)


def _add_common_arguments(parser):
    parser.add_argument(
        "target", help="Logs dir to produce summary from",
    ).complete = completion.DIR
    parser.add_argument(
        "-f", "--file", default=None, help="Name of the generated file."
    )
