import argparse
import json
import logging
import os

from funcy import first

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link, fix_subparsers
from dvc.commands import completion
from dvc.exceptions import DvcException
from dvc.ui import ui
from dvc.utils import format_link

logger = logging.getLogger(__name__)


def _show_json(renderers, split=False):
    from dvc.render.convert import to_json

    result = {
        renderer.name: to_json(renderer, split) for renderer in renderers
    }
    if result:
        ui.write_json(result)


class CmdPlots(CmdBase):
    def _func(self, *args, **kwargs):
        raise NotImplementedError

    def _props(self):
        from dvc.schema import PLOT_PROPS

        # Pass only props specified by user, to not shadow ones from plot def
        props = {p: getattr(self.args, p) for p in PLOT_PROPS}
        return {k: v for k, v in props.items() if v is not None}

    def run(self):
        from pathlib import Path

        from dvc_render import render_html

        from dvc.render.match import match_renderers

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
                targets=self.args.targets, props=self._props()
            )

            if not plots_data:
                ui.error_write(
                    "No plots were loaded, "
                    "visualization file will not be created."
                )

            renderers = match_renderers(
                plots_data=plots_data, out=self.args.out
            )

            if self.args.show_vega:
                renderer = first(filter(lambda r: r.TYPE == "vega", renderers))
                if renderer:
                    ui.write_json(json.loads(renderer.get_filled_template()))
                return 0
            if self.args.json:
                _show_json(renderers, self.args.split)
                return 0

            html_template_path = self.args.html_template
            if not html_template_path:
                html_template_path = self.repo.config.get("plots", {}).get(
                    "html_template", None
                )
                if html_template_path and not os.path.isabs(
                    html_template_path
                ):
                    html_template_path = os.path.join(
                        self.repo.dvc_dir, html_template_path
                    )

            rel: str = self.args.out or "dvc_plots"
            output_file: Path = (Path.cwd() / rel).resolve() / "index.html"

            render_html(
                renderers=renderers,
                output_file=output_file,
                template_path=html_template_path,
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
    TEMPLATES_CHOICES = [
        "simple",
        "linear",
        "confusion",
        "confusion_normalized",
        "scatter",
        "smooth",
    ]

    def run(self):
        from dvc_render.vega_templates import dump_templates

        try:
            out = (
                os.path.join(os.getcwd(), self.args.out)
                if self.args.out
                else self.repo.plots.templates_dir
            )

            targets = [self.args.target] if self.args.target else None
            dump_templates(output=out, targets=targets)
            templates_path = os.path.relpath(out, os.getcwd())
            ui.write(f"Templates have been written into '{templates_path}'.")

            return 0
        except DvcException:
            logger.exception("")
            return 1


def add_parser(subparsers, parent_parser):
    PLOTS_HELP = (
        "Commands to visualize and compare plot metrics in structured files "
        "(JSON, YAML, CSV, TSV)."
    )

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

    SHOW_HELP = "Generate plots from metrics files."
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
        help="Files to visualize (supports any file, "
        "even when not found as `plots` in `dvc.yaml`). "
        "Shows all plots by default.",
    ).complete = completion.FILE
    _add_props_arguments(plots_show_parser)
    _add_output_argument(plots_show_parser)
    _add_ui_arguments(plots_show_parser)
    plots_show_parser.set_defaults(func=CmdPlotsShow)

    PLOTS_DIFF_HELP = (
        "Show multiple versions of plot metrics "
        "by plotting them in a single image."
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
            "Specific plots file(s) to visualize "
            "(even if not found as `plots` in `dvc.yaml`). "
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
        "(has no effect on image-type plots)."
    )
    plots_modify_parser = plots_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=append_doc_link(PLOTS_MODIFY_HELP, "plots/modify"),
        help=PLOTS_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_modify_parser.add_argument(
        "target", help="Metric file to set properties to"
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
        "Write built-in plots templates to a directory "
        "(.dvc/plots by default)."
    )
    plots_templates_parser = plots_subparsers.add_parser(
        "templates",
        parents=[parent_parser],
        description=append_doc_link(TEMPLATES_HELP, "plots/templates"),
        help=TEMPLATES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plots_templates_parser.add_argument(
        "target",
        default=None,
        nargs="?",
        choices=CmdPlotsTemplates.TEMPLATES_CHOICES,
        help="Template to write. Writes all templates by default.",
    )
    _add_output_argument(plots_templates_parser, typ="templates")
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
