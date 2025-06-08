from dvc.cli import formatter
from dvc.cli.utils import append_doc_link, hide_subparsers_from_help
from dvc.commands.experiments import (
    apply,
    branch,
    clean,
    diff,
    exec_run,
    ls,
    pull,
    push,
    queue_worker,
    remove,
    rename,
    run,
    save,
    show,
)

SUB_COMMANDS = [
    apply,
    branch,
    clean,
    diff,
    exec_run,
    ls,
    pull,
    push,
    queue_worker,
    remove,
    rename,
    run,
    save,
    show,
]


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to run and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "exp"),
        formatter_class=formatter.RawDescriptionHelpFormatter,
        help=EXPERIMENTS_HELP,
    )

    experiments_subparsers = experiments_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc experiments CMD --help` to display command-specific help.",
        required=True,
    )

    for cmd in SUB_COMMANDS:
        cmd.add_parser(experiments_subparsers, parent_parser)
    hide_subparsers_from_help(experiments_subparsers)


def add_keep_selection_flag(experiments_subcmd_parser):
    experiments_subcmd_parser.add_argument(
        "--keep",
        action="store_true",
        default=False,
        help="Keep the selected experiments instead of removing them.",
    )


def add_rev_selection_flags(
    experiments_subcmd_parser, command: str, default: bool = True
):
    experiments_subcmd_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help=(
            f"{command} all experiments in the repository "
            "(overrides `--rev` and `--num`)."
        ),
    )
    default_msg = " (HEAD by default)" if default else ""
    msg = (
        f"{command} experiments derived from the specified `<commit>` as "
        f"baseline{default_msg}."
    )
    experiments_subcmd_parser.add_argument(
        "--rev",
        type=str,
        action="append",
        default=None,
        help=msg,
        metavar="<commit>",
    )
    experiments_subcmd_parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1,
        dest="num",
        metavar="<num>",
        help=(
            f"{command} experiments from the last `num` commits "
            "(first parents) starting from the `--rev` baseline. "
            "Give a negative value to include all first-parent commits "
            "(similar to `git log -n`)."
        ),
    )
