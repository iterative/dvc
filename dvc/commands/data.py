import argparse
import logging
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Dict, Iterable, Set

from funcy import chunks, compact, log_durations

from dvc.cli import completion
from dvc.cli.actions import CommaSeparatedArgs
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link, fix_subparsers, hide_subparsers_from_help
from dvc.ui import ui
from dvc.utils import colorize

if TYPE_CHECKING:
    from dvc.repo.data import Status as DataStatus


logger = logging.getLogger(__name__)


class CmdDataStatus(CmdBase):
    COLORS = {
        "not_in_remote": "red",
        "not_in_cache": "red",
        "committed": "green",
        "uncommitted": "yellow",
        "untracked": "cyan",
    }
    LABELS = {
        "not_in_remote": "Not in remote",
        "not_in_cache": "Not in cache",
        "committed": "DVC committed changes",
        "uncommitted": "DVC uncommitted changes",
        "untracked": "Untracked files",
        "unchanged": "DVC unchanged files",
    }
    HINTS = {
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
            items = stage_status
            if isinstance(stage_status, dict):
                items = {
                    file: state
                    for state, files in stage_status.items()
                    for file in files
                }
            if not items:
                continue
            yield stage, items

    @classmethod
    def _show_status(cls, status: "DataStatus") -> int:  # noqa: C901
        git_info = status.pop("git")  # type: ignore[misc]
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
                    ": ".join([state, file]) for file, state in stage_status.items()
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
        with log_durations(
            logger.trace, "in data_status"  # type: ignore[attr-defined]
        ):
            status = self.repo.data_status(
                granular=self.args.granular,
                untracked_files=self.args.untracked_files,
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


class CmdDataLs(CmdBase):
    @staticmethod
    def _show_table(
        d: Iterable[Dict[str, Any]],
        filter_types: Set[str],
        filter_labels: Set[str],
        markdown: bool = False,
    ) -> None:
        from rich.style import Style

        from dvc.compare import TabularData

        td = TabularData(
            columns=["Path", "Type", "Labels", "Description"], fill_value="-"
        )
        for entry in sorted(d, key=itemgetter("path")):
            typ = entry.get("type", "")
            desc = entry.get("desc", "-")
            labels = entry.get("labels", [])

            if filter_types and typ not in filter_types:
                continue
            if filter_labels and filter_labels.isdisjoint(labels):
                continue

            rich_label = ui.rich_text()
            for index, label in enumerate(labels):
                if index:
                    rich_label.append(",")
                style = Style(bold=label in filter_labels, color="green")
                rich_label.append(label, style=style)

            if markdown and desc:
                desc = desc.partition("\n")[0]

            path = ui.rich_text(entry["path"], style="cyan")
            type_style = Style(bold=typ in filter_types, color="yellow")
            typ = ui.rich_text(entry.get("type", ""), style=type_style)
            td.append([path, typ or "-", rich_label or "-", desc])

        td.render(markdown=markdown, rich_table=True)

    def run(self):
        from dvc.repo.data import ls

        filter_labels = set(self.args.labels)
        filter_types = set(self.args.type)
        d = ls(self.repo, targets=self.args.targets, recursive=self.args.recursive)
        self._show_table(
            d,
            filter_labels=filter_labels,
            filter_types=filter_types,
            markdown=self.args.markdown,
        )
        return 0


def add_parser(subparsers, parent_parser):
    data_parser = subparsers.add_parser(
        "data",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    data_subparsers = data_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc data CMD --help` to display command-specific help.",
    )
    fix_subparsers(data_subparsers)

    DATA_STATUS_HELP = (
        "Show changes between the last git commit, the dvcfiles and the workspace."
    )
    data_status_parser = data_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(DATA_STATUS_HELP, "data/status"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=DATA_STATUS_HELP,
    )
    data_status_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    data_status_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        dest="json",
        help=argparse.SUPPRESS,
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
        "--remote-refresh",
        action="store_true",
        default=False,
        help="Refresh remote index.",
    )
    data_status_parser.set_defaults(func=CmdDataStatus)

    DATA_LS_HELP = "List data tracked by DVC with its metadata."
    data_ls_parser = data_subparsers.add_parser(
        "ls",
        aliases=["list"],
        parents=[parent_parser],
        description=append_doc_link(DATA_LS_HELP, "data/ls"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    data_ls_parser.add_argument(
        "--md",
        "--show-md",
        dest="markdown",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    data_ls_parser.add_argument(
        "--type",
        action=CommaSeparatedArgs,
        default=[],
        help="Comma-separated list of type to filter.",
    )
    data_ls_parser.add_argument(
        "--labels",
        action=CommaSeparatedArgs,
        default=[],
        help="Comma-separated list of labels to filter.",
    )
    data_ls_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively list from the specified directories.",
    )
    data_ls_parser.add_argument(
        "targets",
        default=None,
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files, or stage names."
        ),
    ).complete = completion.DVCFILES_AND_STAGE
    data_ls_parser.set_defaults(func=CmdDataLs)
    hide_subparsers_from_help(data_subparsers)
