import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdImport(CmdBase):
    def run(self):
        from dvc.scm import CloneError

        try:
            self.repo.imp(
                self.args.url,
                self.args.path,
                out=self.args.out,
                rev=self.args.rev,
                no_exec=self.args.no_exec,
                no_download=self.args.no_download,
                jobs=self.args.jobs,
                config=self.args.config,
            )
        except CloneError:
            logger.exception("failed to import '%s'", self.args.path)
            return 1
        except DvcException:
            logger.exception(
                "failed to import '%s' from '%s'.",
                self.args.path,
                self.args.url,
            )
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Download file or directory tracked by DVC or by Git "
        "into the workspace, and track it."
    )

    import_parser = subparsers.add_parser(
        "import",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url", help="Location of DVC or Git repository to download from"
    )
    import_parser.add_argument(
        "path", help="Path to a file or directory within the repository"
    ).complete = completion.FILE
    import_parser.add_argument(
        "-o",
        "--out",
        nargs="?",
        help="Destination path to download files to",
        metavar="<path>",
    ).complete = completion.DIR
    import_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    no_download_exec_group = import_parser.add_mutually_exclusive_group()
    no_download_exec_group.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create .dvc file without actually importing target data.",
    )
    no_download_exec_group.add_argument(
        "--no-download",
        action="store_true",
        default=False,
        help=(
            "Create .dvc file including target data hash value(s)"
            " but do not actually download the file(s)."
        ),
    )
    import_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    import_parser.add_argument(
        "--config",
        type=str,
        help=(
            "Path to a config file that will be merged with the config "
            "in the target repository."
        ),
    )
    import_parser.set_defaults(func=CmdImport)
