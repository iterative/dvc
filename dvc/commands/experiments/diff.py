import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.commands.metrics import DEFAULT_PRECISION
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsDiff(CmdBase):
    def run(self):
        try:
            diff = self.repo.experiments.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                all=self.args.all,
                param_deps=self.args.param_deps,
            )
        except DvcException:
            logger.exception("failed to show experiments diff")
            return 1

        if self.args.json:
            ui.write_json(diff)
        elif diff:
            from dvc.compare import show_diff

            precision = self.args.precision or DEFAULT_PRECISION
            diffs = [("metrics", "Metric"), ("params", "Param")]
            for idx, (key, title) in enumerate(diffs):
                if idx:
                    # we are printing tables even in `--quiet` mode
                    # so we should also be printing the "table" separator
                    ui.write(force=True)

                show_diff(
                    diff[key],
                    title=title,
                    markdown=self.args.markdown,
                    no_path=self.args.no_path,
                    on_empty_diff="diff not supported",
                    precision=precision if key == "metrics" else None,
                    a_rev=self.args.a_rev,
                    b_rev=self.args.b_rev,
                )

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_DIFF_HELP = "Show changes between experiments."

    experiments_diff_parser = experiments_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_DIFF_HELP, "exp/diff"),
        help=EXPERIMENTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old experiment to compare (defaults to HEAD)"
    ).complete = completion.EXPERIMENT
    experiments_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New experiment to compare (defaults to the current workspace)",
    ).complete = completion.EXPERIMENT
    experiments_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics/params as well.",
    )
    experiments_diff_parser.add_argument(
        "--param-deps",
        action="store_true",
        default=False,
        help="Show only params that are stage dependencies.",
    )
    experiments_diff_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    experiments_diff_parser.add_argument(
        "--md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show metric/param path.",
    )
    experiments_diff_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics/params to `n` digits precision after the decimal "
            f"point. Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    experiments_diff_parser.set_defaults(func=CmdExperimentsDiff)
