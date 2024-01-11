from dvc.cli import completion, formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.exceptions import DvcException
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdArtifactsGet(CmdBaseNoRepo):
    def run(self):
        from dvc.repo.artifacts import Artifacts
        from dvc.scm import CloneError
        from dvc.ui import ui

        try:
            count, out = Artifacts.get(
                self.args.url,
                name=self.args.name,
                version=self.args.rev,
                stage=self.args.stage,
                force=self.args.force,
                config=self.args.config,
                remote=self.args.remote,
                remote_config=self.args.remote_config,
                out=self.args.out,
            )
            ui.write(f"Downloaded {count} file(s) to '{out}'")
            return 0
        except CloneError:
            logger.exception("failed to get '%s'", self.args.name)
            return 1
        except DvcException:
            logger.exception(
                "failed to get '%s' from '%s'", self.args.name, self.args.url
            )
            return 1


def add_parser(subparsers, parent_parser):
    ARTIFACTS_HELP = "DVC model registry artifact commands."

    artifacts_parser = subparsers.add_parser(
        "artifacts",
        parents=[parent_parser],
        description=append_doc_link(ARTIFACTS_HELP, "artifacts"),
        help=ARTIFACTS_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    artifacts_subparsers = artifacts_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc artifacts CMD --help` to display command-specific help.",
        required=True,
    )

    ARTIFACTS_GET_HELP = "Download an artifact from a DVC project."
    get_parser = artifacts_subparsers.add_parser(
        "get",
        parents=[parent_parser],
        description=append_doc_link(ARTIFACTS_GET_HELP, "artifacts/get"),
        help=ARTIFACTS_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument("url", help="Location of DVC repository to download from")
    get_parser.add_argument(
        "name", help="Name of artifact in the repository"
    ).complete = completion.FILE
    get_parser.add_argument(
        "--rev",
        nargs="?",
        help="Artifact version",
        metavar="<version>",
    )
    get_parser.add_argument(
        "--stage",
        nargs="?",
        help="Artifact stage",
        metavar="<stage>",
    )
    get_parser.add_argument(
        "-o",
        "--out",
        nargs="?",
        help="Destination path to download artifact to",
        metavar="<path>",
    ).complete = completion.DIR
    get_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    get_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Override local file or folder if exists.",
    )
    get_parser.add_argument(
        "--config",
        type=str,
        help=(
            "Path to a config file that will be merged with the config "
            "in the target repository."
        ),
    )
    get_parser.add_argument(
        "--remote",
        type=str,
        help=(
            "Remote name to set as a default in the target repository "
            "(only applicable when downloading from DVC remote)."
        ),
    )
    get_parser.add_argument(
        "--remote-config",
        type=str,
        nargs="*",
        action=DictAction,
        help=(
            "Remote config options to merge with a remote's config (default or one "
            "specified by '--remote') in the target repository (only applicable "
            "when downloading from DVC remote)."
        ),
    )
    get_parser.set_defaults(func=CmdArtifactsGet)
