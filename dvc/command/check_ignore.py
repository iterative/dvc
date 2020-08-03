import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdCheckIgnore(CmdBase):
    def __init__(self, args):
        super().__init__(args)
        self.ignore_filter = self.repo.tree.dvcignore

    def _show_results(self, result):
        if result.match or self.args.non_matching:
            if self.args.details:
                logger.info("{}\t{}".format(result.patterns[-1], result.file))
            else:
                logger.info(result.file)

    def run(self):
        if self.args.non_matching and not self.args.details:
            raise DvcException("--non-matching is only valid with --details")

        if self.args.quiet and self.args.details:
            raise DvcException("cannot both --details and --quiet")

        ret = 1
        for target in self.args.targets:
            result = self.ignore_filter.check_ignore(target)
            self._show_results(result)
            if result.match:
                ret = 0
        return ret


def add_parser(subparsers, parent_parser):
    ADD_HELP = "Debug DVC ignore/exclude files"

    parser = subparsers.add_parser(
        "check-ignore",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "check-ignore"),
        help=ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--details",
        action="store_true",
        default=False,
        help="Show the exclude pattern together with each target path.",
    )
    parser.add_argument(
        "-n",
        "--non-matching",
        action="store_true",
        default=False,
        help="Show the target paths which don’t match any pattern. "
        "Only usable when `--details` is also employed",
    )
    parser.add_argument(
        "targets",
        nargs="+",
        help="Exact or wildcard paths of files or directories to check "
        "ignore patterns.",
    ).complete = completion.FILE
    parser.set_defaults(func=CmdCheckIgnore)
