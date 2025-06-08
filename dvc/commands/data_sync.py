import argparse

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdDataBase(CmdBase):
    def log_summary(self, stats):
        from dvc.ui import ui
        from dvc.utils.humanize import get_summary

        default_msg = "Everything is up to date."

        if not self.args.remote and not self.repo.config["core"].get("remote"):
            ui.warn("No remote provided and no default remote set.")

        ui.write(get_summary(stats.items()) or default_msg)


class CmdDataPull(CmdDataBase):
    def log_summary(self, stats):
        from dvc.commands.checkout import log_changes

        log_changes(stats)
        super().log_summary(stats)

    def run(self):
        from dvc.exceptions import CheckoutError, DvcException

        try:
            stats = self.repo.pull(
                targets=self.args.targets,
                jobs=self.args.jobs,
                remote=self.args.remote,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                with_deps=self.args.with_deps,
                force=self.args.force,
                recursive=self.args.recursive,
                run_cache=self.args.run_cache,
                glob=self.args.glob,
                allow_missing=self.args.allow_missing,
            )
            self.log_summary(stats)
        except (CheckoutError, DvcException) as exc:
            if stats := getattr(exc, "stats", {}):
                self.log_summary(stats)
            logger.exception("failed to pull data from the cloud")
            return 1

        return 0


class CmdDataPush(CmdDataBase):
    def run(self):
        from dvc.exceptions import DvcException

        try:
            processed_files_count = self.repo.push(
                targets=self.args.targets,
                jobs=self.args.jobs,
                remote=self.args.remote,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                with_deps=self.args.with_deps,
                recursive=self.args.recursive,
                run_cache=self.args.run_cache,
                glob=self.args.glob,
            )
            self.log_summary({"pushed": processed_files_count})
        except DvcException:
            logger.exception("failed to push data to the cloud")
            return 1
        return 0


class CmdDataFetch(CmdDataBase):
    def run(self):
        from dvc.exceptions import DvcException

        try:
            processed_files_count = self.repo.fetch(
                targets=self.args.targets,
                jobs=self.args.jobs,
                remote=self.args.remote,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                with_deps=self.args.with_deps,
                recursive=self.args.recursive,
                run_cache=self.args.run_cache,
                max_size=self.args.max_size,
                types=self.args.types,
            )
            self.log_summary({"fetched": processed_files_count})
        except DvcException:
            logger.exception("failed to fetch data from the cloud")
            return 1
        return 0


def shared_parent_parser():
    from dvc.cli.parser import get_parent_parser

    # Parent parser used in pull/push/status
    parent_parser = argparse.ArgumentParser(
        add_help=False, parents=[get_parent_parser()]
    )
    parent_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    parent_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files and stage names."
        ),
    ).complete = completion.DVC_FILE  # type: ignore[attr-defined]

    return parent_parser


def add_parser(subparsers, _parent_parser):
    from dvc.commands.status import CmdDataStatus

    # Pull
    PULL_HELP = "Download tracked files or directories from remote storage."

    pull_parser = subparsers.add_parser(
        "pull",
        parents=[shared_parent_parser()],
        description=append_doc_link(PULL_HELP, "pull"),
        help=PULL_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    pull_parser.add_argument(
        "-r", "--remote", help="Remote storage to pull from", metavar="<name>"
    )
    pull_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Fetch cache for all branches.",
    )
    pull_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Fetch cache for all tags.",
    )
    pull_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Fetch cache for all commits.",
    )
    pull_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Do not prompt when removing working directory files.",
    )
    pull_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Fetch cache for all dependencies of the specified target.",
    )
    pull_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Pull cache for subdirectories of the specified directory.",
    )
    pull_parser.add_argument(
        "--run-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fetch run history for all stages.",
    )
    pull_parser.add_argument(
        "--glob",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    pull_parser.add_argument(
        "--allow-missing",
        action="store_true",
        default=False,
        help="Ignore errors if some of the files or directories are missing.",
    )
    pull_parser.set_defaults(func=CmdDataPull)

    # Push
    PUSH_HELP = "Upload tracked files or directories to remote storage."

    push_parser = subparsers.add_parser(
        "push",
        parents=[shared_parent_parser()],
        description=append_doc_link(PUSH_HELP, "push"),
        help=PUSH_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    push_parser.add_argument(
        "-r", "--remote", help="Remote storage to push to", metavar="<name>"
    )
    push_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Push cache for all branches.",
    )
    push_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Push cache for all tags.",
    )
    push_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Push cache for all commits.",
    )
    push_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Push cache for all dependencies of the specified target.",
    )
    push_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Push cache for subdirectories of specified directory.",
    )
    push_parser.add_argument(
        "--run-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Push run history for all stages.",
    )
    push_parser.add_argument(
        "--glob",
        action="store_true",
        default=False,
        help="Allows targets containing shell-style wildcards.",
    )
    push_parser.set_defaults(func=CmdDataPush)

    # Fetch
    FETCH_HELP = "Download files or directories from remote storage to the cache."

    fetch_parser = subparsers.add_parser(
        "fetch",
        parents=[shared_parent_parser()],
        description=append_doc_link(FETCH_HELP, "fetch"),
        help=FETCH_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    fetch_parser.add_argument(
        "-r", "--remote", help="Remote storage to fetch from", metavar="<name>"
    )
    fetch_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Fetch cache for all branches.",
    )
    fetch_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Fetch cache for all tags.",
    )
    fetch_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Fetch cache for all commits.",
    )
    fetch_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Fetch cache for all dependencies of the specified target.",
    )
    fetch_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Fetch cache for subdirectories of specified directory.",
    )
    fetch_parser.add_argument(
        "--run-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fetch run history for all stages.",
    )
    fetch_parser.add_argument(
        "--max-size",
        type=int,
        help="Fetch data files/directories that are each below specified size (bytes).",
    )
    fetch_parser.add_argument(
        "--type",
        dest="types",
        action="append",
        default=[],
        help=(
            "Only fetch data files/directories that are of a particular "
            "type (metrics, plots)."
        ),
        choices=["metrics", "plots"],
    )
    fetch_parser.set_defaults(func=CmdDataFetch)

    # Status
    STATUS_HELP = "Show changed stages, compare local cache and a remote storage."

    status_parser = subparsers.add_parser(
        "status",
        parents=[shared_parent_parser()],
        description=append_doc_link(STATUS_HELP, "status"),
        help=STATUS_HELP,
        conflict_handler="resolve",
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    status_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help=(
            "Suppresses all output."
            " Exit with 0 if pipelines are up to date, otherwise 1."
        ),
    )
    status_parser.add_argument(
        "-c",
        "--cloud",
        action="store_true",
        default=False,
        help="Show status of a local cache compared to a remote repository.",
    )
    status_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to compare local cache to",
        metavar="<name>",
    )
    status_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help=(
            "Show status of a local cache compared to a remote repository "
            "for all branches."
        ),
    )
    status_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help=(
            "Show status of a local cache compared to a remote repository for all tags."
        ),
    )
    status_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help=(
            "Show status of a local cache compared to a remote repository "
            "for all commits."
        ),
    )
    status_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Show status for all dependencies of the specified target.",
    )
    status_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Show status of all stages in the specified directory.",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show status in JSON format.",
    )
    status_parser.add_argument(
        "--no-updates",
        dest="check_updates",
        action="store_false",
        help="Ignore updates to imported data.",
    )

    status_parser.set_defaults(func=CmdDataStatus)
