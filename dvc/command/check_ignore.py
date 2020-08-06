import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException
from dvc.prompt import ask

logger = logging.getLogger(__name__)


class CmdCheckIgnore(CmdBase):
    def __init__(self, args):
        super().__init__(args)
        self.ignore_filter = self.repo.tree.dvcignore

    def _show_results(self, result):
        if not result.match and not self.args.non_matching:
            return

        if self.args.details:
            patterns = result.patterns
            if not self.args.all:
                patterns = patterns[-1:]

            for pattern in patterns:
                logger.info("{}\t{}".format(pattern, result.file))
        else:
            logger.info(result.file)

    def _check_one_file(self, target):
        result = self.ignore_filter.check_ignore(target)
        self._show_results(result)
        if result.match:
            return 0
        return 1

    def _interactive_mode(self):
        ret = 1
        while True:
            target = ask("")
            if target == "":
                logger.info(
                    "Empty string is not a valid pathspec. Please use . "
                    "instead if you meant to match all paths."
                )
                break
            if not self._check_one_file(target):
                ret = 0
        return ret

    def _normal_mode(self):
        ret = 1
        for target in self.args.targets:
            if not self._check_one_file(target):
                ret = 0
        return ret

    def _check_args(self):
        if not self.args.stdin and not self.args.targets:
            raise DvcException("`targets` or `--stdin` needed")

        if self.args.stdin and self.args.targets:
            raise DvcException("cannot have both `targets` and `--stdin`")

        if self.args.non_matching and not self.args.details:
            raise DvcException(
                "`--non-matching` is only valid with `--details`"
            )

        if self.args.all and not self.args.details:
            raise DvcException("`--all` is only valid with `--details`")

        if self.args.quiet and self.args.details:
            raise DvcException("cannot use both `--details` and `--quiet`")

    def run(self):
        self._check_args()
        if self.args.stdin:
            return self._interactive_mode()
        return self._normal_mode()


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
        "--stdin",
        action="store_true",
        default=False,
        help="Read pathnames from the standard input, one per line, "
        "instead of from the command-line.",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        default=False,
        help="Show all of the patterns match the target paths. "
        "Only usable when `--details` is also employed",
    )
    parser.add_argument(
        "targets",
        nargs="*",
        help="Exact or wildcard paths of files or directories to check "
        "ignore patterns.",
    ).complete = completion.FILE
    parser.set_defaults(func=CmdCheckIgnore)
