from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import CheckoutError
from dvc.ui import ui


def log_changes(stats):
    colors = {
        "modified": "yellow",
        "added": "green",
        "deleted": "red",
    }

    for state, color in colors.items():
        entries = stats.get(state)

        if not entries:
            continue

        for entry in entries:
            ui.write(f"[{color}]{state[0].upper()}", entry, styled=True, sep="\t")


class CmdCheckout(CmdBase):
    def run(self):
        from dvc.utils.humanize import get_summary

        stats, exc = None, None
        try:
            result = self.repo.checkout(
                targets=self.args.targets,
                with_deps=self.args.with_deps,
                force=self.args.force,
                relink=self.args.relink,
                recursive=self.args.recursive,
                allow_missing=self.args.allow_missing,
            )
        except CheckoutError as _exc:
            exc = _exc
            result = exc.result

        if self.args.summary:
            default_message = "No changes."
            stats = result["stats"]
            assert isinstance(stats, dict)
            msg = get_summary(stats.items())
            ui.write(msg or default_message)
        else:
            result.pop("stats", {})
            log_changes(result)

        if exc:
            raise exc

        if self.args.relink:
            msg = "Relinked successfully"
            ui.write(msg)
        return 0


def add_parser(subparsers, parent_parser):
    CHECKOUT_HELP = "Checkout data files from cache."

    checkout_parser = subparsers.add_parser(
        "checkout",
        parents=[parent_parser],
        description=append_doc_link(CHECKOUT_HELP, "checkout"),
        help=CHECKOUT_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    checkout_parser.add_argument(
        "--summary",
        action="store_true",
        default=False,
        help="Show summary of the changes.",
    )
    checkout_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Checkout all dependencies of the specified target.",
    )
    checkout_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Checkout all subdirectories of the specified directory.",
    )
    checkout_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Do not prompt when removing working directory files.",
    )
    checkout_parser.add_argument(
        "--relink",
        action="store_true",
        default=False,
        help="Recreate links or copies from cache to workspace.",
    )
    checkout_parser.add_argument(
        "--allow-missing",
        action="store_true",
        default=False,
        help="Ignore errors if some of the files or directories are missing.",
    )
    checkout_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files and stage names."
        ),
    ).complete = completion.DVC_FILE
    checkout_parser.set_defaults(func=CmdCheckout)
