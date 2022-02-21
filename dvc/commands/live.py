import argparse

from dvc.cli.command import CmdBase
from dvc.cli.utils import fix_subparsers
from dvc.commands import completion
from dvc.ui import ui


class CmdLive(CmdBase):
    UNINITIALIZED = True

    def _run(self, target, revs=None):
        from dvc_render import render_html

        from dvc.render.match import match_renderers

        metrics, plots = self.repo.live.show(target=target, revs=revs)

        if plots:
            from pathlib import Path

            output = Path.cwd() / (self.args.target + "_html") / "index.html"

            renderers = match_renderers(
                plots, templates_dir=self.repo.plots.templates_dir
            )
            index_path = render_html(renderers, output, metrics)
            ui.write(index_path.as_uri())
            return 0
        return 1


class CmdLiveShow(CmdLive):
    def run(self):
        return self._run(self.args.target)


class CmdLiveDiff(CmdLive):
    def run(self):
        return self._run(self.args.target, self.args.revs)


def shared_parent_parser():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "target", help="Logs dir to produce summary from"
    ).complete = completion.DIR
    parent_parser.add_argument(
        "-o",
        "--out",
        default=None,
        help="Destination path to save plots to",
        metavar="<path>",
    ).complete = completion.DIR
    return parent_parser


def add_parser(subparsers, parent_parser):
    LIVE_DESCRIPTION = (
        "Commands to visualize and compare dvclive-produced logs."
    )
    live_parser = subparsers.add_parser(
        "live",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=LIVE_DESCRIPTION,
    )
    live_subparsers = live_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc live CMD --help` to display command-specific help.",
    )

    fix_subparsers(live_subparsers)

    SHOW_HELP = "Visualize dvclive directory content."
    live_show_parser = live_subparsers.add_parser(
        "show",
        parents=[parent_parser, shared_parent_parser()],
        help=SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    live_show_parser.set_defaults(func=CmdLiveShow)

    DIFF_HELP = (
        "Show multiple versions of dvclive data, "
        "by plotting it in single view."
    )
    live_diff_parser = live_subparsers.add_parser(
        "diff",
        parents=[parent_parser, shared_parent_parser()],
        help=DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    live_diff_parser.add_argument(
        "--revs",
        nargs="*",
        default=None,
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    live_diff_parser.set_defaults(func=CmdLiveDiff)
