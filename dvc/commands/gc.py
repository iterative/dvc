import os

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdGC(CmdBase):
    def run(self):  # noqa: C901, PLR0912
        from dvc.repo.gc import _validate_args

        _validate_args(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            all_experiments=self.args.all_experiments,
            commit_date=self.args.commit_date,
            workspace=self.args.workspace,
            rev=self.args.rev,
            num=self.args.num,
            cloud=self.args.cloud,
            not_in_remote=self.args.not_in_remote,
        )

        # Don't prompt during dry run
        if self.args.dry:
            self.args.force = True

        if self.args.rev:
            self.args.num = self.args.num or 1

        msg = "This will remove all cache except items used in "

        msg += "the workspace"
        if self.args.all_commits:
            msg += " and all git commits"
        else:
            if self.args.all_branches and self.args.all_tags:
                msg += " and all git branches and tags"
            elif self.args.all_branches:
                msg += " and all git branches"
            elif self.args.all_tags:
                msg += " and all git tags"
            if self.args.commit_date:
                msg += f" and all git commits before date {self.args.commit_date}"
            if self.args.rev:
                msg += f" and last {self.args.num} commits from {self.args.rev}"

        if self.args.all_experiments:
            msg += " and all experiments"

        if self.args.not_in_remote:
            msg += " that are present in the DVC remote"

        if self.args.repos:
            msg += " of the current and the following repos:"

            for repo_path in self.args.repos:
                msg += "\n  - %s" % os.path.abspath(repo_path)
        else:
            msg += " of the current repo."

        logger.warning(msg)

        msg = "Are you sure you want to proceed?"
        if not self.args.force and not ui.confirm(msg):
            return 1

        self.repo.gc(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            all_experiments=self.args.all_experiments,
            commit_date=self.args.commit_date,
            cloud=self.args.cloud,
            remote=self.args.remote,
            force=self.args.force,
            jobs=self.args.jobs,
            repos=self.args.repos,
            workspace=self.args.workspace,
            rev=self.args.rev,
            num=self.args.num,
            not_in_remote=self.args.not_in_remote,
            dry=self.args.dry,
            skip_failed=self.args.skip_failed,
        )
        return 0


def add_parser(subparsers, parent_parser):
    GC_HELP = "Garbage collect unused objects from cache or remote storage."
    GC_DESCRIPTION = (
        "Removes all files in the cache or a remote which are not in\n"
        "use by the specified Git revisions (defaults to just HEAD)."
    )
    gc_parser = subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(GC_DESCRIPTION, "gc"),
        help=GC_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    gc_parser.add_argument(
        "-w",
        "--workspace",
        action="store_true",
        default=False,
        help="Keep data files used in the current workspace.",
    )
    gc_parser.add_argument(
        "--rev",
        type=str,
        default=None,
        help="Keep data files used in the specified <commit>.",
        metavar="<commit>",
    )
    gc_parser.add_argument(
        "-n",
        "--num",
        type=int,
        dest="num",
        metavar="<num>",
        help=(
            "Keep data files used in the last `num` commits "
            "starting from the `--rev` <commit>. "
            "Only used if `--rev` is also provided. "
            "Defaults to `1`."
        ),
    )
    gc_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Keep data files for the tips of all Git branches.",
    )
    gc_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Keep data files for all Git tags.",
    )
    gc_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Keep data files for all Git commits.",
    )
    gc_parser.add_argument(
        "--date",
        type=str,
        dest="commit_date",
        metavar="<yyyy-mm-dd>",
        default=None,
        help=(
            "Keep cached data referenced in the commits after ( inclusive )"
            " a certain time. Date must match the extended ISO 8601 format "
            "(yyyy-mm-dd)."
        ),
    )
    gc_parser.add_argument(
        "--all-experiments",
        action="store_true",
        default=False,
        help="Keep data files for all experiments.",
    )
    gc_parser.add_argument(
        "--not-in-remote",
        action="store_true",
        default=False,
        help="Keep data files that are not present in the remote.",
    )
    gc_parser.add_argument(
        "-c",
        "--cloud",
        action="store_true",
        default=False,
        help="Collect garbage in remote storage in addition to local cache.",
    )
    gc_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to collect garbage in",
        metavar="<name>",
    )
    gc_parser.add_argument(
        "--skip-failed",
        action="store_true",
        default=False,
        help="Skip revisions that fail when collected.",
    )
    gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection - automatically agree to all prompts.",
    )
    gc_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    gc_parser.add_argument(
        "-p",
        "--projects",
        dest="repos",
        type=str,
        nargs="*",
        help=(
            "Keep data files required by these projects "
            "in addition to the current one. "
            "Useful if you share a single cache across repos."
        ),
        metavar="<paths>",
    )
    gc_parser.add_argument(
        "--dry",
        action="store_true",
        default=False,
        help=("Only print what would get removed without actually removing."),
    )
    gc_parser.set_defaults(func=CmdGC)
