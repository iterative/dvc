import argparse
import logging
from typing import TYPE_CHECKING, List

from funcy import compact

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

if TYPE_CHECKING:
    from dvc.dependency import Dependency
    from dvc.stage import PipelineStage


logger = logging.getLogger(__name__)


class CmdExperimentsInit(CmdBase):
    DEFAULT_NAME = "train"
    CODE = "src"
    DATA = "data"
    MODELS = "models"
    DEFAULT_METRICS = "metrics.json"
    DEFAULT_PARAMS = "params.yaml"
    PLOTS = "plots"
    DEFAULTS = {
        "code": CODE,
        "data": DATA,
        "models": MODELS,
        "metrics": DEFAULT_METRICS,
        "params": DEFAULT_PARAMS,
        "plots": PLOTS,
    }

    def run(self):
        from dvc.commands.stage import parse_cmd

        cmd = parse_cmd(self.args.command)
        if not self.args.interactive and not cmd:
            raise InvalidArgumentError("command is not specified")

        from dvc.repo.experiments.init import init

        defaults = {}
        if not self.args.explicit:
            config = self.repo.config["exp"]
            defaults.update({**self.DEFAULTS, **config})

        cli_args = compact(
            {
                "cmd": cmd,
                "code": self.args.code,
                "data": self.args.data,
                "models": self.args.models,
                "metrics": self.args.metrics,
                "params": self.args.params,
                "plots": self.args.plots,
                "live": self.args.live,
            }
        )

        initialized_stage, initialized_deps, initialized_out_dirs = init(
            self.repo,
            name=self.args.name,
            type=self.args.type,
            defaults=defaults,
            overrides=cli_args,
            interactive=self.args.interactive,
            force=self.args.force,
        )
        self._post_init_display(
            initialized_stage, initialized_deps, initialized_out_dirs
        )
        if self.args.run:
            self.repo.experiments.run(targets=[initialized_stage.addressing])
        return 0

    def _post_init_display(
        self,
        stage: "PipelineStage",
        new_deps: List["Dependency"],
        new_out_dirs: List[str],
    ) -> None:
        from dvc.utils import humanize

        path_fmt = "[green]{}[/green]".format
        if new_deps:
            deps_paths = humanize.join(map(path_fmt, new_deps))
            ui.write(f"Creating dependencies: {deps_paths}", styled=True)

        if new_out_dirs:
            out_dirs_paths = humanize.join(map(path_fmt, new_out_dirs))
            ui.write(
                f"Creating output directories: {out_dirs_paths}", styled=True
            )

        ui.write(
            f"Creating [b]{self.args.name}[/b] stage in [green]dvc.yaml[/]",
            styled=True,
        )
        if stage.outs or not self.args.run:
            # separate the above status-like messages with help/tips section
            ui.write(styled=True)

        if stage.outs:
            outs_paths = humanize.join(map(path_fmt, stage.outs))
            tips = f"Ensure your experiment command creates {outs_paths}."
            ui.write(tips, styled=True)

        if not self.args.run:
            ui.write(
                'You can now run your experiment using [b]"dvc exp run"[/].',
                styled=True,
            )
        else:
            # separate between `exp.run` output and `dvc exp init` output
            ui.write(styled=True)


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_INIT_HELP = "Quickly setup any project to use experiments."
    experiments_init_parser = experiments_subparsers.add_parser(
        "init",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_INIT_HELP, "exp/init"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=EXPERIMENTS_INIT_HELP,
    )
    experiments_init_parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute.",
        metavar="command",
    )
    experiments_init_parser.add_argument(
        "--run",
        action="store_true",
        help="Run the experiment after initializing it",
    )
    experiments_init_parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Prompt for values that are not provided",
    )
    experiments_init_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing stage",
    )
    experiments_init_parser.add_argument(
        "--explicit",
        action="store_true",
        default=False,
        help="Only use the path values explicitly provided",
    )
    experiments_init_parser.add_argument(
        "--name",
        "-n",
        help="Name of the stage to create (default: %(default)s)",
        default=CmdExperimentsInit.DEFAULT_NAME,
    )
    experiments_init_parser.add_argument(
        "--code",
        help="Path to the source file or directory "
        "which your experiments depend"
        f" (default: {CmdExperimentsInit.CODE})",
    )
    experiments_init_parser.add_argument(
        "--data",
        help="Path to the data file or directory "
        "which your experiments depend"
        f" (default: {CmdExperimentsInit.DATA})",
    )
    experiments_init_parser.add_argument(
        "--models",
        help="Path to the model file or directory for your experiments"
        f" (default: {CmdExperimentsInit.MODELS})",
    )
    experiments_init_parser.add_argument(
        "--params",
        help="Path to the parameters file for your experiments"
        f" (default: {CmdExperimentsInit.DEFAULT_PARAMS})",
    )
    experiments_init_parser.add_argument(
        "--metrics",
        help="Path to the metrics file for your experiments"
        f" (default: {CmdExperimentsInit.DEFAULT_METRICS})",
    )
    experiments_init_parser.add_argument(
        "--plots",
        help="Path to the plots file or directory for your experiments"
        f" (default: {CmdExperimentsInit.PLOTS})",
    )
    experiments_init_parser.add_argument(
        "--live",
        help="Path to log dvclive outputs for your experiments",
    )
    experiments_init_parser.add_argument(
        "--type",
        choices=["default", "checkpoint"],
        default="default",
        help="Select type of stage to create (default: %(default)s)",
    )
    experiments_init_parser.set_defaults(func=CmdExperimentsInit)
