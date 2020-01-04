import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBaseNoRepo
from dvc.config import Config


logger = logging.getLogger(__name__)


class CmdConfig(CmdBaseNoRepo):
    def __init__(self, args):
        super().__init__(args)

        self.config = Config(validate=False)

    def run(self):
        section, opt = self.args.name.lower().strip().split(".", 1)

        if self.args.unset:
            self.config.unset(section, opt, level=self.args.level)
        elif self.args.value is None:
            logger.info(self.config.get(section, opt, level=self.args.level))
        else:
            self.config.set(
                section, opt, self.args.value, level=self.args.level
            )

        is_write = self.args.unset or self.args.value is not None
        if is_write and self.args.name == "cache.type":
            logger.warning(
                "You have changed the 'cache.type' option. This doesn't update"
                " any existing workspace file links, but it can be done with:"
                "\n             dvc checkout --relink"
            )

        return 0


parent_config_parser = argparse.ArgumentParser(add_help=False)
parent_config_parser.add_argument(
    "--global",
    dest="level",
    action="store_const",
    const=Config.LEVEL_GLOBAL,
    help="Use global config.",
)
parent_config_parser.add_argument(
    "--system",
    dest="level",
    action="store_const",
    const=Config.LEVEL_SYSTEM,
    help="Use system config.",
)
parent_config_parser.add_argument(
    "--local",
    dest="level",
    action="store_const",
    const=Config.LEVEL_LOCAL,
    help="Use local config.",
)
parent_config_parser.set_defaults(level=Config.LEVEL_REPO)


def add_parser(subparsers, parent_parser):
    CONFIG_HELP = "Get or set config options."

    config_parser = subparsers.add_parser(
        "config",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(CONFIG_HELP, "config"),
        help=CONFIG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    config_parser.add_argument("name", help="Option name.")
    config_parser.add_argument("value", nargs="?", help="Option value.")
    config_parser.set_defaults(func=CmdConfig)
