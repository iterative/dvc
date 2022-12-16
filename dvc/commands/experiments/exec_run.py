import logging

from dvc.cli.command import CmdBaseNoRepo

logger = logging.getLogger(__name__)


class CmdExecutorRun(CmdBaseNoRepo):
    """Run an experiment executor."""

    def run(self):
        from dvc.repo.experiments.executor.base import (
            BaseExecutor,
            ExecutorInfo,
        )
        from dvc.utils.serialize import load_json

        info = ExecutorInfo.from_dict(load_json(self.args.infofile))
        BaseExecutor.reproduce(
            info=info,
            rev="",
            queue=None,
            log_level=logger.getEffectiveLevel(),
            infofile=self.args.infofile,
        )
        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXEC_RUN_HELP = "Run an experiment executor."
    exec_run_parser = experiments_subparsers.add_parser(
        "exec-run",
        parents=[parent_parser],
        description=EXEC_RUN_HELP,
        add_help=False,
    )
    exec_run_parser.add_argument(
        "--infofile",
        help="Path to executor info file",
        default=None,
    )
    exec_run_parser.set_defaults(func=CmdExecutorRun)
