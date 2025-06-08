from typing import Callable

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.commands.ls.ls_colors import LsColors
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


def _get_formatter(with_color: bool = False) -> Callable[[dict], str]:
    def fmt(entry: dict) -> str:
        return entry["path"]

    if with_color:
        ls_colors = LsColors()
        return ls_colors.format

    return fmt


def _format_entry(entry, name, with_size=True, with_hash=False):
    from dvc.utils.humanize import naturalsize

    ret = []
    if with_size:
        size = entry.get("size")
        if size is None or (size <= 0 and entry.get("isdir")):
            size = ""
        else:
            size = naturalsize(size)
        ret.append(size)
    if with_hash:
        md5 = entry.get("md5", "")
        ret.append(md5)
    ret.append(name)
    return ret


def show_entries(entries, with_color=False, with_size=False, with_hash=False):
    fmt = _get_formatter(with_color)
    if with_size or with_hash:
        colalign = ("right",) if with_size else None
        ui.table(
            [
                _format_entry(
                    entry,
                    fmt(entry),
                    with_size=with_size,
                    with_hash=with_hash,
                )
                for entry in entries
            ],
            colalign=colalign,
        )
        return

    # NOTE: this is faster than ui.table for very large number of entries
    ui.write("\n".join(fmt(entry) for entry in entries))


class TreePart:
    Edge = "├── "
    Line = "│   "
    Corner = "└── "
    Blank = "    "


def _build_tree_structure(
    entries, with_color=False, with_size=False, with_hash=False, _depth=0, _prefix=""
):
    rows = []
    fmt = _get_formatter(with_color)

    num_entries = len(entries)
    for i, (name, entry) in enumerate(entries.items()):
        entry["path"] = name
        is_last = i >= num_entries - 1
        tree_part = ""
        if _depth > 0:
            tree_part = TreePart.Corner if is_last else TreePart.Edge

        row = _format_entry(
            entry,
            _prefix + tree_part + fmt(entry),
            with_size=with_size,
            with_hash=with_hash,
        )
        rows.append(row)

        if contents := entry.get("contents"):
            new_prefix = _prefix
            if _depth > 0:
                new_prefix += TreePart.Blank if is_last else TreePart.Line
            new_rows = _build_tree_structure(
                contents,
                with_color=with_color,
                with_size=with_size,
                with_hash=with_hash,
                _depth=_depth + 1,
                _prefix=new_prefix,
            )
            rows.extend(new_rows)

    return rows


def show_tree(entries, with_color=False, with_size=False, with_hash=False):
    import tabulate

    rows = _build_tree_structure(
        entries,
        with_color=with_color,
        with_size=with_size,
        with_hash=with_hash,
    )

    colalign = ("right",) if with_size else None

    _orig = tabulate.PRESERVE_WHITESPACE
    tabulate.PRESERVE_WHITESPACE = True
    try:
        ui.table(rows, colalign=colalign)
    finally:
        tabulate.PRESERVE_WHITESPACE = _orig


class CmdList(CmdBaseNoRepo):
    def _show_tree(self):
        from dvc.repo.ls import ls_tree

        entries = ls_tree(
            self.args.url,
            self.args.path,
            rev=self.args.rev,
            dvc_only=self.args.dvc_only,
            config=self.args.config,
            remote=self.args.remote,
            remote_config=self.args.remote_config,
            maxdepth=self.args.level,
        )
        show_tree(
            entries,
            with_color=True,
            with_size=self.args.size,
            with_hash=self.args.show_hash,
        )
        return 0

    def _show_list(self):
        from dvc.repo import Repo

        entries = Repo.ls(
            self.args.url,
            self.args.path,
            rev=self.args.rev,
            recursive=self.args.recursive,
            dvc_only=self.args.dvc_only,
            config=self.args.config,
            remote=self.args.remote,
            remote_config=self.args.remote_config,
            maxdepth=self.args.level,
        )
        if self.args.json:
            ui.write_json(entries)
        elif entries:
            show_entries(
                entries,
                with_color=True,
                with_size=self.args.size,
                with_hash=self.args.show_hash,
            )
        return 0

    def run(self):
        if self.args.tree and self.args.json:
            raise DvcException("Cannot use --tree and --json options together.")

        try:
            if self.args.tree:
                return self._show_tree()
            return self._show_list()
        except FileNotFoundError:
            logger.exception("")
            return 1
        except DvcException:
            logger.exception("failed to list '%s'", self.args.url)
            return 1


def add_parser(subparsers, parent_parser):
    LIST_HELP = (
        "List repository contents, including files"
        " and directories tracked by DVC and by Git."
    )
    list_parser = subparsers.add_parser(
        "list",
        aliases=["ls"],
        parents=[parent_parser],
        description=append_doc_link(LIST_HELP, "list"),
        help=LIST_HELP,
        formatter_class=formatter.RawTextHelpFormatter,
    )
    list_parser.add_argument("url", help="Location of DVC repository to list")
    list_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Recursively list files.",
    )
    list_parser.add_argument(
        "-T",
        "--tree",
        action="store_true",
        help="Recurse into directories as a tree.",
    )
    list_parser.add_argument(
        "-L",
        "--level",
        metavar="depth",
        type=int,
        help="Limit the depth of recursion.",
    )
    list_parser.add_argument(
        "--dvc-only", action="store_true", help="Show only DVC outputs."
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Show output in JSON format.",
    )
    list_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    list_parser.add_argument(
        "--config",
        type=str,
        help=(
            "Path to a config file that will be merged with the config "
            "in the target repository."
        ),
    )
    list_parser.add_argument(
        "--remote",
        type=str,
        help="Remote name to set as a default in the target repository.",
    )
    list_parser.add_argument(
        "--remote-config",
        type=str,
        nargs="*",
        action=DictAction,
        help=(
            "Remote config options to merge with a remote's config (default or one "
            "specified by '--remote') in the target repository."
        ),
    )
    list_parser.add_argument("--size", action="store_true", help="Show sizes.")
    list_parser.add_argument(
        "--show-hash",
        help="Display hash value for each item.",
        action="store_true",
        default=False,
    )
    list_parser.add_argument(
        "path",
        nargs="?",
        help="Path to directory within the repository to list outputs for",
    ).complete = completion.DIR
    list_parser.set_defaults(func=CmdList)
