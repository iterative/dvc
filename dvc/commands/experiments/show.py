import argparse
import re
from collections.abc import Collection, Iterable
from datetime import date, datetime
from typing import TYPE_CHECKING

from funcy import lmap

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.commands.metrics import DEFAULT_PRECISION
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.ui import ui
from dvc.utils.serialize import encode_exception

if TYPE_CHECKING:
    from dvc.compare import TabularData
    from dvc.ui import RichText

FILL_VALUE = "-"
FILL_VALUE_ERRORED = "!"


logger = logger.getChild(__name__)


experiment_types = {
    "branch_commit": "├──",
    "branch_base": "└──",
    "baseline": "",
}


def prepare_exp_id(kwargs) -> "RichText":
    exp_name = kwargs["Experiment"]
    rev = kwargs["rev"]
    typ = kwargs.get("typ", "baseline")

    if typ == "baseline" or not exp_name:
        text = ui.rich_text(exp_name or rev)
    else:
        text = ui.rich_text.assemble(rev, " [", (exp_name, "bold"), "]")

    parent = kwargs.get("parent")
    suff = f" ({parent})" if parent else ""
    text.append(suff)

    tree = experiment_types[typ]
    pref = f"{tree} " if tree else ""
    return ui.rich_text(pref) + text


def baseline_styler(typ):
    return {"style": "bold"} if typ == "baseline" else {}


def show_experiments(
    td: "TabularData",
    headers: dict[str, Iterable[str]],
    keep: Collection[str] = (),
    drop: Collection[str] = (),
    pager=True,
    csv=False,
    markdown=False,
    **kwargs,
):
    if keep:
        keep_re = re.compile("|".join(keep))
        td.protect(*(col for col in td.keys() if keep_re.match(col)))  # noqa: SIM118

    for col in ("State", "Executor"):
        if td.is_empty(col):
            td.drop(col)

    row_styles = lmap(baseline_styler, td.column("typ"))

    if not csv:
        merge_headers = ["Experiment", "rev", "typ", "parent"]
        td.column("Experiment")[:] = map(prepare_exp_id, td.as_dict(merge_headers))
        td.drop(*merge_headers[1:])

    styles = {
        "Experiment": {"no_wrap": True, "header_style": "black on grey93"},
        "Created": {"header_style": "black on grey93"},
        "State": {"header_style": "black on grey93"},
        "Executor": {"header_style": "black on grey93"},
    }
    header_bg_colors = {
        "metrics": "cornsilk1",
        "params": "light_cyan1",
        "deps": "plum2",
    }
    styles.update(
        {
            header: {
                "justify": "right" if typ == "metrics" else "left",
                "header_style": f"black on {header_bg_colors[typ]}",
                "collapse": idx != 0,
                "no_wrap": typ == "metrics",
            }
            for typ, hs in headers.items()
            for idx, header in enumerate(hs)
        }
    )

    if kwargs.get("only_changed", False):
        td.drop_duplicates("cols", ignore_empty=False)

    cols_to_drop = set()
    if drop:
        drop_re = re.compile("|".join(drop))
        cols_to_drop = {col for col in td.keys() if drop_re.match(col)}  # noqa: SIM118
    td.drop(*cols_to_drop)

    td.render(
        pager=pager,
        borders="horizontals",
        rich_table=True,
        header_styles=styles,
        row_styles=row_styles,
        csv=csv,
        markdown=markdown,
    )


def _normalize_headers(names, count):
    return [
        name if count[name] == 1 else f"{path}:{name}"
        for path in names
        for name in names[path]
    ]


def _format_json(item):
    if isinstance(item, (date, datetime)):
        return item.isoformat()
    return encode_exception(item)


class CmdExperimentsShow(CmdBase):
    def run(self):
        from dvc.repo.experiments.show import tabulate

        try:
            exps = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                hide_queued=self.args.hide_queued,
                hide_failed=self.args.hide_failed,
                hide_workspace=self.args.hide_workspace,
                revs=self.args.rev,
                num=self.args.num,
                sha_only=self.args.sha,
                param_deps=self.args.param_deps,
                fetch_running=self.args.fetch_running,
                force=self.args.force,
            )
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        if self.args.json:
            ui.write_json([exp.dumpd() for exp in exps], default=_format_json)
        else:
            precision = (
                self.args.precision or None if self.args.csv else DEFAULT_PRECISION
            )
            fill_value = "" if self.args.csv else FILL_VALUE
            iso = self.args.csv
            td, headers = tabulate(
                exps,
                precision=precision,
                fill_value=fill_value,
                iso=iso,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
            )

            show_experiments(
                td,
                headers,
                keep=self.args.keep,
                drop=self.args.drop,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
                pager=not self.args.no_pager,
                csv=self.args.csv,
                markdown=self.args.markdown,
                only_changed=self.args.only_changed,
            )
        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_rev_selection_flags

    EXPERIMENTS_SHOW_HELP = "Print experiments."
    experiments_show_parser = experiments_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "exp/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    add_rev_selection_flags(experiments_show_parser, "Show")
    experiments_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show experiments derived from the tip of all Git branches.",
    )
    experiments_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show experiments derived from all Git tags.",
    )
    experiments_show_parser.add_argument(
        "--no-pager",
        action="store_true",
        default=False,
        help="Do not pipe output into a pager.",
    )
    experiments_show_parser.add_argument(
        "--only-changed",
        action="store_true",
        default=False,
        help=(
            "Only show metrics/params with values varying "
            "across the selected experiments."
        ),
    )
    experiments_show_parser.add_argument(
        "--drop",
        action="append",
        default=[],
        help="Remove the columns matching the specified regex pattern.",
        metavar="<regex_pattern>",
    )
    experiments_show_parser.add_argument(
        "--keep",
        action="append",
        default=[],
        help="Preserve the columns matching the specified regex pattern.",
        metavar="<regex_pattern>",
    )
    experiments_show_parser.add_argument(
        "--param-deps",
        action="store_true",
        default=False,
        help="Show only params that are stage dependencies.",
    )
    experiments_show_parser.add_argument(
        "--sort-by",
        help="Sort related experiments by the specified metric or param.",
        metavar="<metric/param>",
    )
    experiments_show_parser.add_argument(
        "--sort-order",
        help="Sort order to use with --sort-by. Defaults to ascending ('asc').",
        choices=("asc", "desc"),
        default="asc",
    )
    experiments_show_parser.add_argument(
        "--sha",
        action="store_true",
        default=False,
        help="Always show git commit SHAs instead of branch/tag names.",
    )
    experiments_show_parser.add_argument(
        "--hide-failed",
        action="store_true",
        default=False,
        help="Hide failed experiments in the table.",
    )
    experiments_show_parser.add_argument(
        "--hide-queued",
        action="store_true",
        default=False,
        help="Hide queued experiments in the table.",
    )
    experiments_show_parser.add_argument(
        "--hide-workspace",
        action="store_true",
        default=False,
        help="Hide workspace row in the table.",
    )
    experiments_show_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Print output in JSON format instead of a human-readable table.",
    )
    experiments_show_parser.add_argument(
        "--csv",
        action="store_true",
        default=False,
        help="Print output in csv format instead of a human-readable table.",
    )
    experiments_show_parser.add_argument(
        "--md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_show_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics/params to `n` digits precision after the decimal "
            f"point. Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    experiments_show_parser.add_argument(
        "--no-fetch",
        dest="fetch_running",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    experiments_show_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force re-collection of experiments instead of loading from exp cache.",
    )
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)
