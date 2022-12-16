import argparse
import json
import logging
import os

from funcy import first

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link, fix_subparsers
from dvc.exceptions import DvcException
from dvc.ui import ui
from dvc.utils import format_link

logger = logging.getLogger(__name__)


def _show_json(renderers, split=False):
    from dvc.render.convert import to_json

    result = {
        renderer.name: to_json(renderer, split) for renderer in renderers
    }
    ui.write_json(result)


def _adjust_vega_renderers(renderers):
    from dvc.render import REVISION_FIELD, VERSION_FIELD
    from dvc_render import VegaRenderer

    for r in renderers:
        if isinstance(r, VegaRenderer):
            if _data_versions_count(r) > 1:
                summary = _summarize_version_infos(r)
                for dp in r.datapoints:
                    vi = dp.pop(VERSION_FIELD, {})
                    keys = list(vi.keys())
                    for key in keys:
                        if not (len(summary.get(key, set())) > 1):
                            vi.pop(key)
                    if vi:
                        dp["rev"] = "::".join(vi.values())
            else:
                for dp in r.datapoints:
                    dp[REVISION_FIELD] = dp[VERSION_FIELD]["revision"]
                    dp.pop(VERSION_FIELD, {})


def _summarize_version_infos(renderer):
    from collections import defaultdict

    from dvc.render import VERSION_FIELD

    result = defaultdict(set)

    for dp in renderer.datapoints:
        for key, value in dp.get(VERSION_FIELD, {}).items():
            result[key].add(value)
    return dict(result)


def _data_versions_count(renderer):
    from itertools import product

    summary = _summarize_version_infos(renderer)
    x = product(summary.get("filename", {None}), summary.get("field", {None}))
    return len(set(x))


class CmdPlots(CmdBase):
    def _func(self, *args, **kwargs):
        raise NotImplementedError

    def _props(self):
        from dvc.schema import PLOT_PROPS

        # Pass only props specified by user, to not shadow ones from plot def
        props = {p: getattr(self.args, p) for p in PLOT_PROPS}
        return {k: v for k, v in props.items() if v is not None}

    def _config_files(self):
        config_files = None
        if self.args.from_config:
            config_files = {self.args.from_config}
        return config_files

    def _html_template_path(self):
        html_template_path = self.args.html_template
        if not html_template_path:
            html_template_path = self.repo.config.get("plots", {}).get(
                "html_template", None
            )
            if html_template_path and not os.path.isabs(html_template_path):
                html_template_path = os.path.join(
                    self.repo.dvc_dir, html_template_path
                )
        return html_template_path

    def run(self):
        from pathlib import Path

        from dvc.render.match import match_defs_renderers
        from dvc_render import render_html

        if self.args.show_vega:
            if not self.args.targets:
                logger.error("please specify a target for `--show-vega`")
                return 1
            if len(self.args.targets) > 1:
                logger.error(
                    "you can only specify one target for `--show-vega`"
                )
                return 1
            if self.args.json:
                logger.error(
                    "'--show-vega' and '--json' are mutually exclusive "
                    "options."
                )
                return 1

        try:
            plots_data = self._func(
                targets=self.args.targets,
                props=self._props(),
                config_files=self._config_files(),
            )

            if not plots_data:
                ui.error_write(
                    "No plots were loaded, "
                    "visualization file will not be created."
                )

            out: str = self.args.out or self.repo.config.get("plots", {}).get(
                "out_dir", "dvc_plots"
            )

            renderers_out = (
                out if self.args.json else os.path.join(out, "static")
            )

            renderers = match_defs_renderers(
                data=plots_data,
                out=renderers_out,
                templates_dir=self.repo.plots.templates_dir,
            )
            if self.args.json:
                _show_json(renderers, self.args.split)
                return 0

            _adjust_vega_renderers(renderers)
            if self.args.show_vega:
                renderer = first(filter(lambda r: r.TYPE == "vega", renderers))
                if renderer:
                    ui.write_json(json.loads(renderer.get_filled_template()))
                return 0

            output_file: Path = (Path.cwd() / out).resolve() / "index.html"

            if renderers:
                render_html(
                    renderers=renderers,
                    output_file=output_file,
                    template_path=self._html_template_path(),
                )

                ui.write(output_file.as_uri())
                auto_open = self.repo.config["plots"].get("auto_open", False)
                if self.args.open or auto_open:
                    if not auto_open:
                        ui.write(
                            "To enable auto opening, you can run:\n"
                            "\n"
                            "\tdvc config plots.auto_open true"
                        )
                    return ui.open_browser(output_file)

            return 0

        except DvcException:
            logger.exception("")
            return 1


class CmdPlotsShow(CmdPlots):
    UNINITIALIZED = True

    def _func(self, *args, **kwargs):
        return self.repo.plots.show(*args, **kwargs)


class CmdPlotsDiff(CmdPlots):
    UNINITIALIZED = True

    def _func(self, *args, **kwargs):
        return self.repo.plots.diff(
            *args,
            revs=self.args.revisions,
            experiment=self.args.experiment,
            **kwargs,
        )


class CmdPlotsModify(CmdPlots):
    def run(self):
        self.repo.plots.modify(
            self.args.target, props=self._props(), unset=self.args.unset
        )
        return 0


class CmdPlotsTemplates(CmdBase):
    def run(self):
        from dvc.exceptions import InvalidArgumentError
        from dvc_render.vega_templates import TEMPLATES

        try:
            target = self.args.template
            if target:
                for template in TEMPLATES:
                    if target == template.DEFAULT_NAME:
                        ui.write_json(template.DEFAULT_CONTENT)
                        return 0
                raise InvalidArgumentError(f"Unexpected template: {target}.")

            else:
                for template in TEMPLATES:
                    ui.write(template.DEFAULT_NAME)

            return 0
        except DvcException:
            logger.exception("")
            return 1


def add_parser(subparsers, parent_parser):
    PLOTS_HELP = "Commands to visualize and compare plot data."

    plots_parser = subparsers.add_parser(
        "plots",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_HELP, "plots"),
        help=PLOTS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_subparsers = plots_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc plots CMD --help` to display command-specific help.",
    )

    fix_subparsers(plots_subparsers)

    SHOW_HELP = (
        "Generate plots from target files or from `plots`"
        " definitions in `dvc.yaml`."
    )
    plots_show_parser = plots_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(SHOW_HELP, "plots/show"),
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_show_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Plots files or plot IDs from `dvc.yaml` to visualize. "
            "Shows all plots by default."
        ),
    ).complete = completion.FILE
    _add_props_arguments(plots_show_parser)
    _add_output_argument(plots_show_parser)
    _add_ui_arguments(plots_show_parser)
    plots_show_parser.set_defaults(func=CmdPlotsShow)

    PLOTS_DIFF_HELP = (
        "Show multiple versions of a plot by overlaying them "
        "in a single image."
    )
    plots_diff_parser = plots_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_DIFF_HELP, "plots/diff"),
        help=PLOTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Specific plots to visualize. "
            "Accepts any file path or plot name from `dvc.yaml` file. "
            "Shows all tracked plots by default."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    plots_diff_parser.add_argument(
        "-e",
        "--experiment",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    plots_diff_parser.add_argument(
        "revisions", nargs="*", default=None, help="Git commits to plot from"
    )
    _add_props_arguments(plots_diff_parser)
    _add_output_argument(plots_diff_parser)
    _add_ui_arguments(plots_diff_parser)
    plots_diff_parser.set_defaults(func=CmdPlotsDiff)

    PLOTS_MODIFY_HELP = (
        "Modify display properties of data-series plots "
        "defined in stages (has no effect on image plots)."
    )
    plots_modify_parser = plots_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_MODIFY_HELP, "plots/modify"),
        help=PLOTS_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_modify_parser.add_argument(
        "target",
        help="Plots file to set properties for (defined at the stage level).",
    ).complete = completion.FILE
    _add_props_arguments(plots_modify_parser)
    plots_modify_parser.add_argument(
        "--unset",
        nargs="*",
        metavar="<property>",
        help="Unset one or more display properties.",
    )
    plots_modify_parser.set_defaults(func=CmdPlotsModify)

    TEMPLATES_HELP = (
        "List built-in plots templates or show JSON specification for one."
    )
    plots_templates_parser = plots_subparsers.add_parser(
        "templates",
        parents=[parent_parser],
        description=append_doc_link(TEMPLATES_HELP, "plots/templates"),
        help=TEMPLATES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_templates_parser.add_argument(
        "template",
        default=None,
        nargs="?",
        help=(
            "Template for which to show JSON specification. "
            "List all template names by default."
        ),
    )
    plots_templates_parser.set_defaults(func=CmdPlotsTemplates)


def _add_props_arguments(parser):
    parser.add_argument(
        "-t",
        "--template",
        nargs="?",
        default=None,
        help=(
            "Special JSON or HTML schema file to inject with the data. "
            "See {}".format(
                format_link("https://man.dvc.org/plots#plot-templates")
            )
        ),
        metavar="<path>",
    ).complete = completion.FILE
    parser.add_argument(
        "-x", default=None, help="Field name for X axis.", metavar="<field>"
    )
    parser.add_argument(
        "-y", default=None, help="Field name for Y axis.", metavar="<field>"
    )
    parser.add_argument(
        "--no-header",
        action="store_false",
        dest="header",
        default=None,  # Use default None to distinguish when it's not used
        help="Provided CSV or TSV datafile does not have a header.",
    )
    parser.add_argument(
        "--title", default=None, metavar="<text>", help="Plot title."
    )
    parser.add_argument(
        "--x-label", default=None, help="X axis label", metavar="<text>"
    )
    parser.add_argument(
        "--y-label", default=None, help="Y axis label", metavar="<text>"
    )


def _add_output_argument(parser, typ="plots"):
    parser.add_argument(
        "-o",
        "--out",
        default=None,
        help=f"Directory to save {typ} to.",
        metavar="<path>",
    ).complete = completion.DIR


def _add_ui_arguments(parser):
    parser.add_argument(
        "--show-vega",
        action="store_true",
        default=False,
        help="Show output in Vega format.",
    )
    parser.add_argument(
        "--json",
        "--show-json",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--split", action="store_true", default=False, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--open",
        action="store_true",
        default=False,
        help="Open plot file directly in the browser.",
    )
    parser.add_argument(
        "--html-template",
        default=None,
        help="Custom HTML template for VEGA visualization.",
        metavar="<path>",
    )
    parser.add_argument(
        "--from-config",
        default=None,
        metavar="<path>",
        help=argparse.SUPPRESS,
    )
