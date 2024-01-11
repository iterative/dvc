from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdAdd(CmdBase):
    def validate_args(self) -> None:
        from dvc.exceptions import InvalidArgumentError

        args = self.args
        invalid_opt = None

        if args.to_remote or args.out:
            message = "{option} can't be used with "
            message += "--to-remote" if args.to_remote else "--out"
            if len(args.targets) != 1:
                invalid_opt = "multiple targets"
            elif args.glob:
                invalid_opt = "--glob option"
            elif args.no_commit:
                invalid_opt = "--no-commit option"
        else:
            message = "{option} can't be used without --to-remote"
            if args.remote:
                invalid_opt = "--remote"
            elif args.remote_jobs:
                invalid_opt = "--remote-jobs"

        if invalid_opt is not None:
            raise InvalidArgumentError(message.format(option=invalid_opt))

    def run(self):
        from dvc.exceptions import DvcException, InvalidArgumentError

        try:
            self.validate_args()
        except InvalidArgumentError:
            logger.exception("")
            return 1

        try:
            self.repo.add(
                self.args.targets,
                no_commit=self.args.no_commit,
                glob=self.args.glob,
                out=self.args.out,
                remote=self.args.remote,
                to_remote=self.args.to_remote,
                remote_jobs=self.args.remote_jobs,
                force=self.args.force,
            )
        except FileNotFoundError:
            logger.exception("")
            return 1
        except DvcException:
            logger.exception("")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    ADD_HELP = "Track data files or directories with DVC."

    parser = subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(ADD_HELP, "add"),
        help=ADD_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    parser.add_argument(
        "--glob",
        action="store_true",
        default=False,
        help="Allows targets containing shell-style wildcards.",
    )
    parser.add_argument(
        "-o",
        "--out",
        help="Destination path to put files to.",
        metavar="<path>",
    )
    parser.add_argument(
        "--to-remote",
        action="store_true",
        default=False,
        help="Download it directly to the remote",
    )
    parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to download to",
        metavar="<name>",
    )
    parser.add_argument(
        "--remote-jobs",
        type=int,
        help=(
            "Only used along with '--to-remote'. "
            "Number of jobs to run simultaneously "
            "when pushing data to remote."
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Override local file or folder if exists.",
    )
    parser.add_argument(
        "targets", nargs="+", help="Input files/directories to add."
    ).complete = completion.FILE
    parser.set_defaults(func=CmdAdd)
