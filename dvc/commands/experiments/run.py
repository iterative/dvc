import argparse

from dvc.cli import formatter
from dvc.cli.utils import append_doc_link
from dvc.commands.repro import CmdRepro
from dvc.commands.repro import add_arguments as add_repro_arguments
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdExperimentsRun(CmdRepro):
    def run(self):
        self.repo.experiments.run(
            name=self.args.name,
            queue=self.args.queue,
            run_all=self.args.run_all,
            jobs=self.args.jobs,
            params=self.args.set_param,
            tmp_dir=self.args.tmp_dir,
            copy_paths=self.args.copy_paths,
            message=self.args.message,
            no_hydra=self.args.no_hydra,
            **self._common_kwargs,
        )

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_RUN_HELP = "Run an experiment."
    experiments_run_parser = experiments_subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_RUN_HELP, "exp/run"),
        help=EXPERIMENTS_RUN_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    _add_run_common(experiments_run_parser)
    experiments_run_parser.set_defaults(func=CmdExperimentsRun)


def _add_run_common(parser):
    """Add common args for 'exp run'."""
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
        default=1,
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
    parser.add_argument(
        "-C",
        "--copy-paths",
        action="append",
        default=[],
        help=(
            "List of ignored or untracked paths to copy into the temp directory."
            " Only used if `--temp` or `--queue` is specified."
        ),
    )
    parser.add_argument(
        "-m",
        "--message",
        type=str,
        default=None,
        help="Custom commit message to use when committing the experiment.",
    )
    parser.add_argument(
        "--no-hydra",
        action="store_true",
        default=False,
        help=(
            "Disables automatically updating `params.yaml` with Hydra configuration. "
            " You can still use `--set-param` to update individual params if needed."
            " Default is False."
        ),
    )
    parser.add_argument("-M", dest="message", help=argparse.SUPPRESS)  # obsolete
