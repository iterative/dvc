from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdParamsDiff(CmdBase):
    UNINITIALIZED = True

    def run(self):
        import os
        from os.path import relpath

        diff_result = self.repo.params.diff(
            a_rev=self.args.a_rev,
            b_rev=self.args.b_rev,
            targets=self.args.targets,
            all=self.args.all,
            deps_only=self.args.deps,
        )

        errored = [rev for rev, err in diff_result.get("errors", {}).items() if err]
        if errored:
            ui.error_write(
                "DVC failed to load some metrics for following revisions:"
                f" '{', '.join(errored)}'."
            )

        start = relpath(os.getcwd(), self.repo.root_dir)
        diff = diff_result.get("diff", {})
        diff = {relpath(path, start): result for path, result in diff.items()}

        if self.args.json:
            ui.write_json(diff)
        else:
            from dvc.compare import show_diff

            show_diff(
                diff,
                title="Param",
                markdown=self.args.markdown,
                no_path=self.args.no_path,
                show_changes=False,
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
            )

        return 0


def add_parser(subparsers, parent_parser):
    PARAMS_HELP = "Commands to display params."

    params_parser = subparsers.add_parser(
        "params",
        parents=[parent_parser],
        description=append_doc_link(PARAMS_HELP, "params"),
        help=PARAMS_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    params_subparsers = params_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc params CMD --help` to display command-specific help.",
        required=True,
    )

    PARAMS_DIFF_HELP = (
        "Show changes in params between commits in the DVC repository, or "
        "between a commit and the workspace."
    )
    params_diff_parser = params_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PARAMS_DIFF_HELP, "params/diff"),
        help=PARAMS_DIFF_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    params_diff_parser.add_argument(
        "a_rev",
        nargs="?",
        default="HEAD",
        help="Old Git commit to compare (defaults to HEAD)",
    )
    params_diff_parser.add_argument(
        "b_rev",
        default="workspace",
        nargs="?",
        help="New Git commit to compare (defaults to the current workspace)",
    )
    params_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Specific params file(s) to compare "
            "(even if not found as `params` in `dvc.yaml`). "
            "Shows all tracked params by default."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    params_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged params as well.",
    )
    params_diff_parser.add_argument(
        "--deps",
        action="store_true",
        default=False,
        help="Show only params that are stage dependencies.",
    )
    params_diff_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    params_diff_parser.add_argument(
        "--md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Show tabulated output in the Markdown format (GFM).",
    )
    params_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show params path.",
    )
    params_diff_parser.set_defaults(func=CmdParamsDiff)
