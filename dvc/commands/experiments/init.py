import argparse
import logging

from funcy import compact

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsInit(CmdBase):
    DEFAULT_NAME = "train"
    CODE = "src"
    DATA = "data"
    MODELS = "models"
    DEFAULT_METRICS = "metrics.json"
    DEFAULT_PARAMS = "params.yaml"
    PLOTS = "plots"
    DVCLIVE = "dvclive"
    DEFAULTS = {
        "code": CODE,
        "data": DATA,
        "models": MODELS,
        "metrics": DEFAULT_METRICS,
        "params": DEFAULT_PARAMS,
        "plots": PLOTS,
        "live": DVCLIVE,
    }
    EXP_LINK = "https://s.dvc.org/g/exp/run"

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

        initialized_stage = init(
            self.repo,
            name=self.args.name,
            type=self.args.type,
            defaults=defaults,
            overrides=cli_args,
            interactive=self.args.interactive,
            force=self.args.force,
        )

        text = ui.rich_text.assemble(
            "\n" if self.args.interactive else "",
            "Created ",
            (self.args.name, "bright_blue"),
            " stage in ",
            ("dvc.yaml", "green"),
            ".",
        )
        if not self.args.run:
            text.append_text(
                ui.rich_text.assemble(
                    " To run, use ",
                    ('"dvc exp run"', "green"),
                    ".\nSee ",
                    (self.EXP_LINK, "repr.url"),
                    ".",
                )
            )

        ui.write(text, styled=True)
        if self.args.run:
            return self.repo.experiments.run(
                targets=[initialized_stage.addressing]
            )

        return 0


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
        help="Path to log dvclive outputs for your experiments"
        f" (default: {CmdExperimentsInit.DVCLIVE})",
    )
    experiments_init_parser.add_argument(
        "--type",
        choices=["default", "dl"],
        default="default",
        help="Select type of stage to create (default: %(default)s)",
    )
    experiments_init_parser.set_defaults(func=CmdExperimentsInit)
