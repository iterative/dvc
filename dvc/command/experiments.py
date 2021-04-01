import argparse
import logging
from collections import Counter, OrderedDict, defaultdict
from collections.abc import Mapping
from datetime import date, datetime
from itertools import groupby
from typing import Dict, Iterable, Optional

import dvc.prompt as prompt
from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.metrics import DEFAULT_PRECISION
from dvc.command.repro import CmdRepro
from dvc.command.repro import add_arguments as add_repro_arguments
from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.utils.flatten import flatten

logger = logging.getLogger(__name__)


SHOW_MAX_WIDTH = 1024


def _filter_name(names, label, filter_strs):
    ret = defaultdict(dict)
    path_filters = defaultdict(list)

    for filter_s in filter_strs:
        path, _, name = filter_s.rpartition(":")
        path_filters[path].append(tuple(name.split(".")))

    for path, filters in path_filters.items():
        if path:
            match_paths = [path]
        else:
            match_paths = names.keys()
        for length, groups in groupby(filters, len):
            for group in groups:
                for match_path in match_paths:
                    possible_names = [
                        tuple(name.split(".")) for name in names[match_path]
                    ]
                    matches = [
                        name
                        for name in possible_names
                        if name[:length] == group
                    ]
                    if not matches:
                        name = ".".join(group)
                        raise InvalidArgumentError(
                            f"'{name}' does not match any known {label}"
                        )
                    ret[match_path].update(
                        {".".join(match): None for match in matches}
                    )

    return ret


def _filter_names(
    names: Dict[str, Dict[str, None]],
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

    if include:
        ret = _filter_name(names, label, include)
    else:
        ret = names

    if exclude:
        to_remove = _filter_name(names, label, exclude)
        for path in to_remove:
            if path in ret:
                for key in to_remove[path]:
                    if key in ret[path]:
                        del ret[path][key]

    return ret


def _update_names(names, items):
    for name, item in items:
        if isinstance(item, dict):
            item = flatten(item)
            names[name].update({key: None for key in item})


def _collect_names(all_experiments, **kwargs):
    metric_names = defaultdict(dict)
    param_names = defaultdict(dict)

    for _, experiments in all_experiments.items():
        for exp in experiments.values():
            _update_names(metric_names, exp.get("metrics", {}).items())
            _update_names(param_names, exp.get("params", {}).items())

    metric_names = _filter_names(
        metric_names,
        "metrics",
        kwargs.get("include_metrics"),
        kwargs.get("exclude_metrics"),
    )
    param_names = _filter_names(
        (param_names),
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
    from dvc.scm.git import Git

    if sort_by:
        sort_path, sort_name, sort_type = _sort_column(
            sort_by, metric_names, param_names
        )
        reverse = sort_order == "desc"
        experiments = _sort_exp(
            experiments, sort_path, sort_name, sort_type, reverse
        )

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


def _sort_column(sort_by, metric_names, param_names):
    path, _, sort_name = sort_by.rpartition(":")
    matches = set()

    if path:
        if path in metric_names and sort_name in metric_names[path]:
            matches.add((path, sort_name, "metrics"))
        if path in param_names and sort_name in param_names[path]:
            matches.add((path, sort_name, "params"))
    else:
        for path in metric_names:
            if sort_name in metric_names[path]:
                matches.add((path, sort_name, "metrics"))
        for path in param_names:
            if sort_name in param_names[path]:
                matches.add((path, sort_name, "params"))

    if len(matches) == 1:
        return matches.pop()
    if len(matches) > 1:
        raise InvalidArgumentError(
            "Ambiguous sort column '{}' matched '{}'".format(
                sort_by,
                ", ".join([f"{path}:{name}" for path, name, _ in matches]),
            )
        )
    raise InvalidArgumentError(f"Unknown sort column '{sort_by}'")


def _sort_exp(experiments, sort_path, sort_name, typ, reverse):
    def _sort(item):
        rev, exp = item
        tip = exp.get("checkpoint_tip")
        if tip and tip != rev:
            # Sort checkpoint experiments by tip commit
            return _sort((tip, experiments[tip]))
        data = exp.get(typ, {}).get(sort_path, {})
        val = flatten(data).get(sort_name)
        return (val is None, val)

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
    if isinstance(val, Mapping):
        return {k: _format_field(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_format_field(x) for x in val]
    return str(val)


def _extend_row(row, names, items, precision):
    from rich.text import Text

    if not items:
        for keys in names.values():
            row.extend(["-"] * len(keys))
        return

    for fname, item in items:
        if isinstance(item, dict):
            item = flatten(item)
        else:
            item = {fname: item}
        for name in names[fname]:
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


def _parse_filter_list(param_list):
    ret = []
    for param_str in param_list:
        path, _, param_str = param_str.rpartition(":")
        if path:
            ret.extend(f"{path}:{param}" for param in param_str.split(","))
        else:
            ret.extend(param_str.split(","))
    return ret


def _experiments_table(all_experiments, **kwargs):
    from dvc.utils.table import Table

    include_metrics = _parse_filter_list(kwargs.pop("include_metrics", []))
    exclude_metrics = _parse_filter_list(kwargs.pop("exclude_metrics", []))
    include_params = _parse_filter_list(kwargs.pop("include_params", []))
    exclude_params = _parse_filter_list(kwargs.pop("exclude_params", []))

    metric_names, param_names = _collect_names(
        all_experiments,
        include_metrics=include_metrics,
        exclude_metrics=exclude_metrics,
        include_params=include_params,
        exclude_params=exclude_params,
    )

    table = Table()
    table.add_column(
        "Experiment", no_wrap=True, header_style="black on grey93"
    )
    if not kwargs.get("no_timestamp", False):
        table.add_column("Created", header_style="black on grey93")
    _add_data_columns(
        table,
        metric_names,
        justify="right",
        no_wrap=True,
        header_style="black on cornsilk1",
    )
    _add_data_columns(
        table, param_names, justify="left", header_style="black on light_cyan1"
    )

    for base_rev, experiments in all_experiments.items():
        for row, _, in _collect_rows(
            base_rev, experiments, metric_names, param_names, **kwargs,
        ):
            table.add_row(*row)

    return table


def _add_data_columns(table, names, **kwargs):
    count = Counter(
        name for path in names for name in names[path] for path in names
    )
    first = True
    for path in names:
        for name in names[path]:
            col_name = name if count[name] == 1 else f"{path}:{name}"
            kwargs["collapse"] = False if first else True
            table.add_column(col_name, **kwargs)
            first = False


def _format_json(item):
    if isinstance(item, (date, datetime)):
        return item.isoformat()
    raise TypeError


class CmdExperimentsShow(CmdBase):
    def run(self):
        from rich.console import Console

        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                sha_only=self.args.sha,
                num=self.args.num,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(all_experiments, default=_format_json))
                return 0

            if self.args.precision is None:
                precision = DEFAULT_PRECISION
            else:
                precision = self.args.precision

            table = _experiments_table(
                all_experiments,
                include_metrics=self.args.include_metrics,
                exclude_metrics=self.args.exclude_metrics,
                include_params=self.args.include_params,
                exclude_params=self.args.exclude_params,
                no_timestamp=self.args.no_timestamp,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
                precision=precision,
            )

            console = Console()
            if self.args.no_pager:
                console.print(table)
            else:
                from dvc.utils.pager import DvcPager

                # NOTE: rich does not have native support for unlimited width
                # via pager. we override rich table compression by setting
                # console width to the full width of the table
                console_options = console.options
                console_options.max_width = SHOW_MAX_WIDTH
                measurement = table.__rich_measure__(console, console_options)
                console._width = (  # pylint: disable=protected-access
                    measurement.maximum
                )
                with console.pager(pager=DvcPager(), styles=True):
                    console.print(table)

        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


class CmdExperimentsApply(CmdBase):
    def run(self):

        self.repo.experiments.apply(
            self.args.experiment, force=self.args.force
        )

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
        from dvc.command.metrics import _show_metrics

        if self.args.reset and self.args.checkpoint_resume:
            raise InvalidArgumentError(
                "--reset and --rev are mutually exclusive."
            )

        if self.args.reset:
            logger.info("Any existing checkpoints will be reset and re-run.")

        results = self.repo.experiments.run(
            name=self.args.name,
            queue=self.args.queue,
            run_all=self.args.run_all,
            jobs=self.args.jobs,
            params=self.args.set_param,
            checkpoint_resume=self.args.checkpoint_resume,
            reset=self.args.reset,
            tmp_dir=self.args.tmp_dir,
            **self._repro_kwargs,
        )

        if self.args.metrics and results:
            metrics = self.repo.metrics.show(revs=list(results))
            metrics.pop("workspace", None)
            logger.info(_show_metrics(metrics))

        return 0


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "or `--all-commits` needs to be set."
        )


class CmdExperimentsGC(CmdRepro):
    def run(self):
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

        self.repo.experiments.branch(self.args.experiment, self.args.branch)

        return 0


class CmdExperimentsList(CmdBase):
    def run(self):
        names_only = self.args.names_only
        exps = self.repo.experiments.ls(
            rev=self.args.rev,
            git_remote=self.args.git_remote,
            all_=self.args.all,
        )
        for baseline in exps:
            tag = self.repo.scm.describe(baseline)
            if not tag:
                branch = self.repo.scm.describe(baseline, base="refs/heads")
                if branch:
                    tag = branch.split("/")[-1]
            name = tag if tag else baseline[:7]
            if not names_only:
                print(f"{name}:")
            for exp_name in exps[baseline]:
                indent = "" if names_only else "\t"
                print(f"{indent}{exp_name}")

        return 0


class CmdExperimentsPush(CmdBase):
    def run(self):

        self.repo.experiments.push(
            self.args.git_remote,
            self.args.experiment,
            force=self.args.force,
            push_cache=self.args.push_cache,
            dvc_remote=self.args.dvc_remote,
            jobs=self.args.jobs,
            run_cache=self.args.run_cache,
        )

        logger.info(
            "Pushed experiment '%s' to Git remote '%s'.",
            self.args.experiment,
            self.args.git_remote,
        )
        if not self.args.push_cache:
            logger.info(
                "To push cached outputs for this experiment to DVC remote "
                "storage, re-run this command without '--no-cache'."
            )

        return 0


class CmdExperimentsPull(CmdBase):
    def run(self):

        self.repo.experiments.pull(
            self.args.git_remote,
            self.args.experiment,
            force=self.args.force,
            pull_cache=self.args.pull_cache,
            dvc_remote=self.args.dvc_remote,
            jobs=self.args.jobs,
            run_cache=self.args.run_cache,
        )

        logger.info(
            "Pulled experiment '%s' from Git remote '%s'. ",
            self.args.experiment,
            self.args.git_remote,
        )
        if not self.args.pull_cache:
            logger.info(
                "To pull cached outputs for this experiment from DVC remote "
                "storage, re-run this command without '--no-cache'."
            )

        return 0


class CmdExperimentsRemove(CmdBase):
    def run(self):

        self.repo.experiments.remove(
            exp_names=self.args.experiment, queue=self.args.queue,
        )

        return 0


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to run and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "exp"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=EXPERIMENTS_HELP,
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
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "exp/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
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
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Show experiments derived from all Git commits.",
    )
    experiments_show_parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1,
        dest="num",
        metavar="<num>",
        help="Show the last `num` commits from HEAD.",
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
        description=append_doc_link(EXPERIMENTS_APPLY_HELP, "exp/apply"),
        help=EXPERIMENTS_APPLY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_apply_parser.add_argument(
        "--no-force",
        action="store_false",
        dest="force",
        help="Fail if this command would overwrite conflicting changes.",
    )
    experiments_apply_parser.add_argument(
        "experiment", help="Experiment to be applied.",
    ).complete = completion.EXPERIMENT
    experiments_apply_parser.set_defaults(func=CmdExperimentsApply)

    EXPERIMENTS_DIFF_HELP = (
        "Show changes between experiments in the DVC repository."
    )
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
        description=append_doc_link(EXPERIMENTS_RUN_HELP, "exp/run"),
        help=EXPERIMENTS_RUN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _add_run_common(experiments_run_parser)
    experiments_run_parser.add_argument(
        "-r",
        "--rev",
        type=str,
        dest="checkpoint_resume",
        help=(
            "Continue the specified checkpoint experiment. "
            "(Only required for explicitly resuming checkpoints in queued "
            "or temp dir runs.)"
        ),
        metavar="<experiment_rev>",
    ).complete = completion.EXPERIMENT
    experiments_run_parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset existing checkpoints and restart the experiment.",
    )
    experiments_run_parser.set_defaults(func=CmdExperimentsRun)

    EXPERIMENTS_GC_HELP = "Garbage collect unneeded experiments."
    EXPERIMENTS_GC_DESCRIPTION = (
        "Removes all experiments which are not derived from the specified"
        "Git revisions."
    )
    experiments_gc_parser = experiments_subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_GC_DESCRIPTION, "exp/gc"),
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
        description=append_doc_link(EXPERIMENTS_BRANCH_HELP, "exp/branch"),
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

    EXPERIMENTS_LIST_HELP = "List local and remote experiments."
    experiments_list_parser = experiments_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_LIST_HELP, "exp/list"),
        help=EXPERIMENTS_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_list_parser.add_argument(
        "--rev",
        type=str,
        default=None,
        help=(
            "List experiments derived from the specified revision. "
            "Defaults to HEAD if neither `--rev` nor `--all` are specified."
        ),
        metavar="<rev>",
    )
    experiments_list_parser.add_argument(
        "--all", action="store_true", help="List all experiments.",
    )
    experiments_list_parser.add_argument(
        "--names-only",
        action="store_true",
        help="Only output experiment names (without parent commits).",
    )
    experiments_list_parser.add_argument(
        "git_remote",
        nargs="?",
        default=None,
        help=(
            "Optional Git remote name or Git URL. If provided, experiments "
            "from the specified Git repository will be listed instead of "
            "local experiments."
        ),
        metavar="[<git_remote>]",
    )
    experiments_list_parser.set_defaults(func=CmdExperimentsList)

    EXPERIMENTS_PUSH_HELP = "Push a local experiment to a Git remote."
    experiments_push_parser = experiments_subparsers.add_parser(
        "push",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_PUSH_HELP, "exp/push"),
        help=EXPERIMENTS_PUSH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_push_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Replace experiment in the Git remote if it already exists.",
    )
    experiments_push_parser.add_argument(
        "--no-cache",
        action="store_false",
        dest="push_cache",
        help=(
            "Do not push cached outputs for this experiment to DVC remote "
            "storage."
        ),
    )
    experiments_push_parser.add_argument(
        "-r",
        "--remote",
        dest="dvc_remote",
        metavar="<name>",
        help="Name of the DVC remote to use when pushing cached outputs.",
    )
    experiments_push_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        metavar="<number>",
        help=(
            "Number of jobs to run simultaneously when pushing to DVC remote "
            "storage."
        ),
    )
    experiments_push_parser.add_argument(
        "--run-cache",
        action="store_true",
        default=False,
        help="Push run history for all stages.",
    )
    experiments_push_parser.add_argument(
        "git_remote",
        help="Git remote name or Git URL.",
        metavar="<git_remote>",
    )
    experiments_push_parser.add_argument(
        "experiment", help="Experiment to push.", metavar="<experiment>",
    ).complete = completion.EXPERIMENT
    experiments_push_parser.set_defaults(func=CmdExperimentsPush)

    EXPERIMENTS_PULL_HELP = "Pull an experiment from a Git remote."
    experiments_pull_parser = experiments_subparsers.add_parser(
        "pull",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_PULL_HELP, "exp/pull"),
        help=EXPERIMENTS_PULL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_pull_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Replace local experiment already exists.",
    )
    experiments_pull_parser.add_argument(
        "--no-cache",
        action="store_false",
        dest="pull_cache",
        help=(
            "Do not pull cached outputs for this experiment from DVC remote "
            "storage."
        ),
    )
    experiments_pull_parser.add_argument(
        "-r",
        "--remote",
        dest="dvc_remote",
        metavar="<name>",
        help="Name of the DVC remote to use when pulling cached outputs.",
    )
    experiments_pull_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        metavar="<number>",
        help=(
            "Number of jobs to run simultaneously when pulling from DVC "
            "remote storage."
        ),
    )
    experiments_pull_parser.add_argument(
        "--run-cache",
        action="store_true",
        default=False,
        help="Pull run history for all stages.",
    )
    experiments_pull_parser.add_argument(
        "git_remote",
        help="Git remote name or Git URL.",
        metavar="<git_remote>",
    )
    experiments_pull_parser.add_argument(
        "experiment", help="Experiment to pull.", metavar="<experiment>",
    )
    experiments_pull_parser.set_defaults(func=CmdExperimentsPull)

    EXPERIMENTS_REMOVE_HELP = "Remove local experiments."
    experiments_remove_parser = experiments_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_REMOVE_HELP, "exp/remove"),
        help=EXPERIMENTS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_remove_parser.add_argument(
        "--queue", action="store_true", help="Remove all queued experiments.",
    )
    experiments_remove_parser.add_argument(
        "experiment",
        nargs="*",
        help="Experiments to remove.",
        metavar="<experiment>",
    )
    experiments_remove_parser.set_defaults(func=CmdExperimentsRemove)


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
        metavar="<name>",
    )
    parser.add_argument(
        "-S",
        "--set-param",
        action="append",
        default=[],
        help="Use the specified param value when reproducing pipelines.",
        metavar="[<filename>:]<param_name>=<param_value>",
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
        help="Execute all experiments in the run queue. Implies --temp.",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help="Run the specified number of experiments at a time in parallel.",
        metavar="<number>",
    )
    parser.add_argument(
        "--temp",
        action="store_true",
        dest="tmp_dir",
        help=(
            "Run this experiment in a separate temporary directory instead of "
            "your workspace."
        ),
    )
