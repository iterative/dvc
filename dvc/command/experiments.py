import argparse
import io
import logging
import os
from collections import OrderedDict
from collections.abc import Mapping
from datetime import date, datetime
from itertools import groupby
from typing import Iterable, Optional

import dvc.prompt as prompt
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.metrics import DEFAULT_PRECISION
from dvc.command.repro import CmdRepro
from dvc.command.repro import add_arguments as add_repro_arguments
from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.repo.experiments import Experiments
from dvc.scm.git import Git
from dvc.utils.flatten import flatten

logger = logging.getLogger(__name__)


def _filter_names(
    names: Iterable,
    label: str,
    include: Optional[Iterable],
    exclude: Optional[Iterable],
):
    if include and exclude:
        intersection = set(include) & set(exclude)
        if intersection:
            values = ", ".join(intersection)
            raise InvalidArgumentError(
                f"'{values}' specified in both --include-{label} and"
                f" --exclude-{label}"
            )

    names = [tuple(name.split(".")) for name in names]

    def _filter(filters, update_func):
        filters = [tuple(name.split(".")) for name in filters]
        for length, groups in groupby(filters, len):
            for group in groups:
                matches = [name for name in names if name[:length] == group]
                if not matches:
                    name = ".".join(group)
                    raise InvalidArgumentError(
                        f"'{name}' does not match any known {label}"
                    )
                update_func({match: None for match in matches})

    if include:
        ret: OrderedDict = OrderedDict()
        _filter(include, ret.update)
    else:
        ret = OrderedDict({name: None for name in names})

    if exclude:
        _filter(exclude, ret.difference_update)  # type: ignore[attr-defined]

    return [".".join(name) for name in ret]


def _update_names(names, items):
    for name, item in items:
        if isinstance(item, dict):
            item = flatten(item)
            names.update(item.keys())
        else:
            names[name] = None


def _collect_names(all_experiments, **kwargs):
    metric_names = set()
    param_names = set()

    for _, experiments in all_experiments.items():
        for exp in experiments.values():
            _update_names(metric_names, exp.get("metrics", {}).items())
            _update_names(param_names, exp.get("params", {}).items())

    metric_names = _filter_names(
        sorted(metric_names),
        "metrics",
        kwargs.get("include_metrics"),
        kwargs.get("exclude_metrics"),
    )
    param_names = _filter_names(
        sorted(param_names),
        "params",
        kwargs.get("include_params"),
        kwargs.get("exclude_params"),
    )

    return metric_names, param_names


def _collect_rows(
    base_rev,
    experiments,
    metric_names,
    param_names,
    precision=DEFAULT_PRECISION,
    no_timestamp=False,
    sort_by=None,
    sort_order=None,
):
    if sort_by:
        if sort_by in metric_names:
            sort_type = "metrics"
        elif sort_by in param_names:
            sort_type = "params"
        else:
            raise InvalidArgumentError(f"Unknown sort column '{sort_by}'")
        reverse = sort_order == "desc"
        experiments = _sort_exp(experiments, sort_by, sort_type, reverse)

    new_checkpoint = True
    for i, (rev, exp) in enumerate(experiments.items()):
        row = []
        style = None
        queued = "*" if exp.get("queued", False) else ""

        tip = exp.get("checkpoint_tip")
        parent = ""
        if rev == "baseline":
            if Git.is_sha(base_rev):
                name_rev = base_rev[:7]
            else:
                name_rev = base_rev
            name = exp.get("name", name_rev)
            row.append(f"{name}")
            style = "bold"
        else:
            if tip:
                parent_rev = exp.get("checkpoint_parent", "")
                parent_exp = experiments.get(parent_rev, {})
                parent_tip = parent_exp.get("checkpoint_tip")
                if tip == parent_tip:
                    if new_checkpoint:
                        tree = "│ ╓"
                    else:
                        tree = "│ ╟"
                    new_checkpoint = False
                else:
                    if parent_rev == base_rev:
                        tree = "├─╨"
                    else:
                        tree = "│ ╟"
                        parent = f" ({parent_rev[:7]})"
                    new_checkpoint = True
            else:
                if i < len(experiments) - 1:
                    tree = "├──"
                else:
                    tree = "└──"
                new_checkpoint = True
            name = exp.get("name", rev[:7])
            row.append(f"{tree} {queued}{name}{parent}")

        if not no_timestamp:
            row.append(_format_time(exp.get("timestamp")))

        _extend_row(
            row, metric_names, exp.get("metrics", {}).items(), precision
        )
        _extend_row(row, param_names, exp.get("params", {}).items(), precision)

        yield row, style


def _sort_exp(experiments, sort_by, typ, reverse):
    def _sort(item):
        rev, exp = item
        tip = exp.get("checkpoint_tip")
        if tip and tip != rev:
            # Sort checkpoint experiments by tip commit
            return _sort((tip, experiments[tip]))
        for fname, item in exp.get(typ, {}).items():
            if isinstance(item, dict):
                item = flatten(item)
            else:
                item = {fname: item}
            if sort_by in item:
                val = item[sort_by]
                return (val is None, val)
        return (True, None)

    ret = OrderedDict()
    if "baseline" in experiments:
        ret["baseline"] = experiments.pop("baseline")

    ret.update(sorted(experiments.items(), key=_sort, reverse=reverse))
    return ret


def _format_time(timestamp):
    if timestamp is None:
        return "-"
    if timestamp.date() == date.today():
        fmt = "%I:%M %p"
    else:
        fmt = "%b %d, %Y"
    return timestamp.strftime(fmt)


def _format_field(val, precision=DEFAULT_PRECISION):
    if isinstance(val, float):
        fmt = f"{{:.{precision}g}}"
        return fmt.format(val)
    elif isinstance(val, Mapping):
        return {k: _format_field(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [_format_field(x) for x in val]
    return str(val)


def _extend_row(row, names, items, precision):
    from rich.text import Text

    if not items:
        row.extend(["-"] * len(names))
        return

    for fname, item in items:
        if isinstance(item, dict):
            item = flatten(item)
        else:
            item = {fname: item}
        for name in names:
            if name in item:
                value = item[name]
                if value is None:
                    text = "-"
                else:
                    # wrap field data in rich.Text, otherwise rich may
                    # interpret unescaped braces from list/dict types as rich
                    # markup tags
                    text = Text(str(_format_field(value, precision)))
                row.append(text)
            else:
                row.append("-")


def _parse_list(param_list):
    ret = []
    for param_str in param_list:
        # we don't care about filename prefixes for show, silently
        # ignore it if provided to keep usage consistent with other
        # metric/param list command options
        _, _, param_str = param_str.rpartition(":")
        ret.extend(param_str.split(","))
    return ret


def _show_experiments(all_experiments, console, **kwargs):
    from rich.table import Table

    include_metrics = _parse_list(kwargs.pop("include_metrics", []))
    exclude_metrics = _parse_list(kwargs.pop("exclude_metrics", []))
    include_params = _parse_list(kwargs.pop("include_params", []))
    exclude_params = _parse_list(kwargs.pop("exclude_params", []))

    metric_names, param_names = _collect_names(
        all_experiments,
        include_metrics=include_metrics,
        exclude_metrics=exclude_metrics,
        include_params=include_params,
        exclude_params=exclude_params,
    )

    table = Table()
    table.add_column("Experiment", no_wrap=True)
    if not kwargs.get("no_timestamp", False):
        table.add_column("Created")
    for name in metric_names:
        table.add_column(name, justify="right", no_wrap=True)
    for name in param_names:
        table.add_column(name, justify="left")

    for base_rev, experiments in all_experiments.items():
        for row, _, in _collect_rows(
            base_rev, experiments, metric_names, param_names, **kwargs,
        ):
            table.add_row(*row)

    console.print(table)


def _format_json(item):
    if isinstance(item, (date, datetime)):
        return item.isoformat()
    raise TypeError


class CmdExperimentsShow(CmdBase):
    def run(self):
        from rich.console import Console

        from dvc.utils.pager import pager

        if not self.repo.experiments:
            return 0

        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                sha_only=self.args.sha,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(all_experiments, default=_format_json))
                return 0

            if self.args.no_pager:
                console = Console()
            else:
                # Note: rich does not currently include a native way to force
                # infinite width for use with a pager
                console = Console(
                    file=io.StringIO(), force_terminal=True, width=9999
                )

            if self.args.precision is None:
                precision = DEFAULT_PRECISION
            else:
                precision = self.args.precision

            _show_experiments(
                all_experiments,
                console,
                include_metrics=self.args.include_metrics,
                exclude_metrics=self.args.exclude_metrics,
                include_params=self.args.include_params,
                exclude_params=self.args.exclude_params,
                no_timestamp=self.args.no_timestamp,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
                precision=precision,
            )

            if not self.args.no_pager:
                pager(console.file.getvalue())
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


class CmdExperimentsApply(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        self.repo.experiments.apply(self.args.experiment)

        return 0


def _show_diff(
    diff,
    title="",
    markdown=False,
    no_path=False,
    old=False,
    precision=DEFAULT_PRECISION,
):
    from dvc.utils.diff import table

    rows = []
    for fname, diff_ in diff.items():
        sorted_diff = OrderedDict(sorted(diff_.items()))
        for item, change in sorted_diff.items():
            row = [] if no_path else [fname]
            row.append(item)
            if old:
                row.append(_format_field(change.get("old"), precision))
            row.append(_format_field(change["new"], precision))
            row.append(
                _format_field(
                    change.get("diff", "diff not supported"), precision
                )
            )
            rows.append(row)

    header = [] if no_path else ["Path"]
    header.append(title)
    if old:
        header.extend(["Old", "New"])
    else:
        header.append("Value")
    header.append("Change")

    return table(header, rows, markdown)


class CmdExperimentsDiff(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        try:
            diff = self.repo.experiments.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                all=self.args.all,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(diff))
            else:
                if self.args.precision is None:
                    precision = DEFAULT_PRECISION
                else:
                    precision = self.args.precision

                diffs = [("metrics", "Metric"), ("params", "Param")]
                for key, title in diffs:
                    table = _show_diff(
                        diff[key],
                        title=title,
                        markdown=self.args.show_md,
                        no_path=self.args.no_path,
                        old=self.args.old,
                        precision=precision,
                    )
                    if table:
                        logger.info(table)
                        logger.info("")

        except DvcException:
            logger.exception("failed to show experiments diff")
            return 1

        return 0


class CmdExperimentsRun(CmdRepro):
    def run(self):
        if not self.repo.experiments:
            return 0

        saved_dir = os.path.realpath(os.curdir)
        os.chdir(self.args.cwd)

        self.repo.experiments.run(
            name=self.args.name,
            queue=self.args.queue,
            run_all=self.args.run_all,
            jobs=self.args.jobs,
            params=self.args.params,
            checkpoint_resume=self.args.checkpoint_resume,
            **self._repro_kwargs,
        )

        os.chdir(saved_dir)
        return 0


class CmdExperimentsGC(CmdRepro):
    def run(self):
        from dvc.repo.gc import _raise_error_if_all_disabled

        if not self.repo.experiments:
            return 0

        _raise_error_if_all_disabled(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            workspace=self.args.workspace,
        )

        msg = "This will remove all experiments except those derived from "

        msg += "the workspace"
        if self.args.all_commits:
            msg += " and all git commits"
        elif self.args.all_branches and self.args.all_tags:
            msg += " and all git branches and tags"
        elif self.args.all_branches:
            msg += " and all git branches"
        elif self.args.all_tags:
            msg += " and all git tags"
        msg += " of the current repo."
        if self.args.queued:
            msg += " Run queued experiments will be preserved."
        if self.args.queued:
            msg += " Run queued experiments will be removed."

        logger.warning(msg)

        msg = "Are you sure you want to proceed?"
        if not self.args.force and not prompt.confirm(msg):
            return 1

        removed = self.repo.experiments.gc(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            workspace=self.args.workspace,
            queued=self.args.queued,
        )

        if removed:
            logger.info(
                f"Removed {removed} experiments. To remove unused cache files "
                "use 'dvc gc'."
            )
        else:
            logger.info("No experiments to remove.")
        return 0


class CmdExperimentsBranch(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        self.repo.experiments.branch(self.args.experiment, self.args.branch)

        return 0


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to display and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "experiments"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    experiments_subparsers = experiments_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc experiments CMD --help` to display "
        "command-specific help.",
    )

    fix_subparsers(experiments_subparsers)

    EXPERIMENTS_SHOW_HELP = "Print experiments."
    experiments_show_parser = experiments_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "experiments/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show metrics for all branches.",
    )
    experiments_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show metrics for all tags.",
    )
    experiments_show_parser.add_argument(
        "--all-commits",
        action="store_true",
        default=False,
        help="Show metrics for all commits.",
    )
    experiments_show_parser.add_argument(
        "--no-pager",
        action="store_true",
        default=False,
        help="Do not pipe output into a pager.",
    )
    experiments_show_parser.add_argument(
        "--include-metrics",
        action="append",
        default=[],
        help="Include the specified metrics in output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-metrics",
        action="append",
        default=[],
        help="Exclude the specified metrics from output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--include-params",
        action="append",
        default=[],
        help="Include the specified params in output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-params",
        action="append",
        default=[],
        help="Exclude the specified params from output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--sort-by",
        help="Sort related experiments by the specified metric or param.",
        metavar="<metric/param>",
    )
    experiments_show_parser.add_argument(
        "--sort-order",
        help="Sort order to use with --sort-by.",
        choices=("asc", "desc"),
        default="asc",
    )
    experiments_show_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        default=False,
        help="Do not show experiment timestamps.",
    )
    experiments_show_parser.add_argument(
        "--sha",
        action="store_true",
        default=False,
        help="Always show git commit SHAs instead of branch/tag names.",
    )
    experiments_show_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Print output in JSON format instead of a human-readable table.",
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
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)

    EXPERIMENTS_APPLY_HELP = (
        "Apply the changes from an experiment to your workspace."
    )
    experiments_apply_parser = experiments_subparsers.add_parser(
        "apply",
        parents=[parent_parser],
        description=append_doc_link(
            EXPERIMENTS_APPLY_HELP, "experiments/apply"
        ),
        help=EXPERIMENTS_APPLY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_apply_parser.add_argument(
        "experiment", help="Experiment to be applied.",
    )
    experiments_apply_parser.set_defaults(func=CmdExperimentsApply)

    EXPERIMENTS_DIFF_HELP = (
        "Show changes between experiments in the DVC repository."
    )
    experiments_diff_parser = experiments_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_DIFF_HELP, "experiments/diff"),
        help=EXPERIMENTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old experiment to compare (defaults to HEAD)"
    )
    experiments_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New experiment to compare (defaults to the current workspace)",
    )
    experiments_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics/params as well.",
    )
    experiments_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    experiments_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_diff_parser.add_argument(
        "--old",
        action="store_true",
        default=False,
        help="Show old metric/param value.",
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

    EXPERIMENTS_RUN_HELP = (
        "Reproduce complete or partial experiment pipelines."
    )
    experiments_run_parser = experiments_subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_RUN_HELP, "experiments/run"),
        help=EXPERIMENTS_RUN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_run_common(experiments_run_parser)
    experiments_run_parser.add_argument(
        "--checkpoint-resume", type=str, default=None, help=argparse.SUPPRESS,
    )
    experiments_run_parser.set_defaults(func=CmdExperimentsRun)

    EXPERIMENTS_RESUME_HELP = "Resume checkpoint experiments."
    experiments_resume_parser = experiments_subparsers.add_parser(
        "resume",
        parents=[parent_parser],
        aliases=["res"],
        description=append_doc_link(
            EXPERIMENTS_RESUME_HELP, "experiments/resume"
        ),
        help=EXPERIMENTS_RESUME_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_run_common(experiments_resume_parser)
    experiments_resume_parser.add_argument(
        "-r",
        "--rev",
        type=str,
        default=Experiments.LAST_CHECKPOINT,
        dest="checkpoint_resume",
        help=(
            "Continue the specified checkpoint experiment. "
            "If no experiment revision is provided, "
            "the most recently run checkpoint experiment will be used."
        ),
        metavar="<experiment_rev>",
    )
    experiments_resume_parser.set_defaults(func=CmdExperimentsRun)

    EXPERIMENTS_GC_HELP = "Garbage collect unneeded experiments."
    EXPERIMENTS_GC_DESCRIPTION = (
        "Removes all experiments which are not derived from the specified"
        "Git revisions."
    )
    experiments_gc_parser = experiments_subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(
            EXPERIMENTS_GC_DESCRIPTION, "experiments/gc"
        ),
        help=EXPERIMENTS_GC_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_gc_parser.add_argument(
        "-w",
        "--workspace",
        action="store_true",
        default=False,
        help="Keep experiments derived from the current workspace.",
    )
    experiments_gc_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Keep experiments derived from the tips of all Git branches.",
    )
    experiments_gc_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Keep experiments derived from all Git tags.",
    )
    experiments_gc_parser.add_argument(
        "--all-commits",
        action="store_true",
        default=False,
        help="Keep experiments derived from all Git commits.",
    )
    experiments_gc_parser.add_argument(
        "--queued",
        action="store_true",
        default=False,
        help=(
            "Keep queued experiments (experiments run queue will be cleared "
            "by default)."
        ),
    )
    experiments_gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection - automatically agree to all prompts.",
    )
    experiments_gc_parser.set_defaults(func=CmdExperimentsGC)

    EXPERIMENTS_BRANCH_HELP = "Promote an experiment to a Git branch."
    experiments_branch_parser = experiments_subparsers.add_parser(
        "branch",
        parents=[parent_parser],
        description=append_doc_link(
            EXPERIMENTS_BRANCH_HELP, "experiments/branch"
        ),
        help=EXPERIMENTS_BRANCH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_branch_parser.add_argument(
        "experiment", help="Experiment to be promoted.",
    )
    experiments_branch_parser.add_argument(
        "branch", help="Git branch name to use.",
    )
    experiments_branch_parser.set_defaults(func=CmdExperimentsBranch)


def _add_run_common(parser):
    """Add common args for 'exp run' and 'exp resume'."""
    # inherit arguments from `dvc repro`
    add_repro_arguments(parser)
    parser.add_argument(
        "-n",
        "--name",
        default=None,
        help=(
            "Human-readable experiment name. If not specified, a name will "
            "be auto-generated."
        ),
    )
    parser.add_argument(
        "--params",
        action="append",
        default=[],
        help="Use the specified param values when reproducing pipelines.",
        metavar="[<filename>:]<params_list>",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        default=False,
        help="Stage this experiment in the run queue for future execution.",
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        default=False,
        help="Execute all experiments in the run queue.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help="Run the specified number of experiments at a time in parallel.",
        metavar="<number>",
    )
