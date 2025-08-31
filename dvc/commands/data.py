from typing import TYPE_CHECKING, ClassVar

from funcy import chunks, compact, log_durations

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui
from dvc.utils import colorize

if TYPE_CHECKING:
    from dvc.repo.data import GitInfo
    from dvc.repo.data import Status as DataStatus


logger = logger.getChild(__name__)


class CmdDataStatus(CmdBase):
    COLORS: ClassVar[dict[str, str]] = {
        "not_in_remote": "red",
        "not_in_cache": "red",
        "committed": "green",
        "uncommitted": "yellow",
        "untracked": "cyan",
    }
    LABELS: ClassVar[dict[str, str]] = {
        "not_in_remote": "Not in remote",
        "not_in_cache": "Not in cache",
        "committed": "DVC committed changes",
        "uncommitted": "DVC uncommitted changes",
        "untracked": "Untracked files",
        "unchanged": "DVC unchanged files",
    }
    HINTS: ClassVar[dict[str, tuple[str, ...]]] = {
        "not_in_remote": ('use "dvc push <file>..." to upload files',),
        "not_in_cache": ('use "dvc fetch <file>..." to download files',),
        "committed": ("git commit the corresponding dvc files to update the repo",),
        "uncommitted": (
            'use "dvc commit <file>..." to track changes',
            'use "dvc checkout <file>..." to discard changes',
        ),
        "untracked": (
            (
                'use "git add <file> ..." or '
                '"dvc add <file>..." to commit to git or to dvc'
            ),
        ),
        "git_dirty": (
            'there are {}changes not tracked by dvc, use "git status" to see',
        ),
    }

    @staticmethod
    def _process_status(status: "DataStatus"):
        """Flatten stage status, and filter empty stage status contents."""
        for stage, stage_status in status.items():
            if not stage_status or (
                isinstance(stage_status, dict) and not any(stage_status.values())
            ):
                continue
            yield stage, stage_status

    @classmethod
    def _show_status(cls, status: "DataStatus") -> int:  # noqa: C901
        git_info: GitInfo = status.pop("git")  # type: ignore[misc]
        result = dict(cls._process_status(status))
        if not result:
            no_changes = "No changes"
            if git_info.get("is_empty", False):
                no_changes += " in an empty git repo"
            ui.write(f"{no_changes}.")

        for idx, (stage, stage_status) in enumerate(result.items()):
            if idx:
                ui.write()

            label = cls.LABELS.get(stage, stage.capitalize() + " files")
            header = f"{label}:"
            color = cls.COLORS.get(stage, None)

            ui.write(header)
            if hints := cls.HINTS.get(stage):
                for hint in hints:
                    ui.write(f"  ({hint})")

            if isinstance(stage_status, dict):
                items = [
                    f"{state}: "
                    + (
                        " -> ".join(change.values())
                        if isinstance(change, dict)
                        else change
                    )
                    for state, changes in stage_status.items()
                    for change in changes
                ]
            else:
                items = stage_status

            tabs = "\t".expandtabs(8)
            for chunk in chunks(1000, items):
                out = "\n".join(tabs + item for item in chunk)
                ui.write(colorize(out, color))

        if (hints := cls.HINTS.get("git_dirty")) and git_info.get("is_dirty"):
            for hint in hints:
                message = hint.format("other " if result else "")
                ui.write(f"[blue]({message})[/]", styled=True)
        return 0

    def run(self) -> int:
        with log_durations(logger.trace, "in data_status"):
            status = self.repo.data_status(
                targets=self.args.targets,
                granular=self.args.granular,
                untracked_files=self.args.untracked_files,
                remote=self.args.remote,
                not_in_remote=self.args.not_in_remote,
                remote_refresh=self.args.remote_refresh,
            )

        if not self.args.unchanged:
            status.pop("unchanged")  # type: ignore[misc]
        if self.args.untracked_files == "no":
            status.pop("untracked")
        if self.args.json:
            status.pop("git")  # type: ignore[misc]
            ui.write_json(compact(status))
            return 0
        return self._show_status(status)


def add_parser(subparsers, parent_parser):
    data_help = "Commands related to data management."
    data_parser = subparsers.add_parser(
        "data",
        parents=[parent_parser],
        description=append_doc_link(data_help, "data/status"),
        help=data_help,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    data_subparsers = data_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc data CMD --help` to display command-specific help.",
        required=True,
    )

    DATA_STATUS_HELP = (
        "Show changes between the last git commit, the dvcfiles and the workspace."
    )
    data_status_parser = data_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(DATA_STATUS_HELP, "data/status"),
        formatter_class=formatter.RawDescriptionHelpFormatter,
        help=DATA_STATUS_HELP,
    )
    data_status_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files and stage names."
        ),
    ).complete = completion.FILE  # type: ignore[attr-defined]
    data_status_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    data_status_parser.add_argument(
        "--granular",
        action="store_true",
        default=False,
        help="Show granular file-level info for DVC-tracked directories.",
    )
    data_status_parser.add_argument(
        "--unchanged",
        action="store_true",
        default=False,
        help="Show unmodified DVC-tracked files.",
    )
    data_status_parser.add_argument(
        "--untracked-files",
        choices=["no", "all"],
        default="no",
        const="all",
        nargs="?",
        help="Show untracked files.",
    )
    data_status_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to check (only applicable with --not-in-remote).",
        metavar="<name>",
    ).complete = completion.REMOTE
    data_status_parser.add_argument(
        "--not-in-remote",
        action="store_true",
        default=False,
        help="Show files not in remote.",
    )
    data_status_parser.add_argument(
        "--no-remote-refresh",
        dest="remote_refresh",
        action="store_false",
        help="Use cached remote index (don't check remote).",
    )
    data_status_parser.set_defaults(func=CmdDataStatus)
