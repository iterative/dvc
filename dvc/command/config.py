import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.config import Config, ConfigError

logger = logging.getLogger(__name__)


class CmdConfig(CmdBaseNoRepo):
    def __init__(self, args):
        super().__init__(args)

        self.config = Config(validate=False)

    def run(self):
        section, opt = self.args.name.lower().strip().split(".", 1)

        if self.args.value is None and not self.args.unset:
            conf = self.config.load_one(self.args.level)
            self._check(conf, section, opt)
            logger.info(conf[section][opt])
            return 0

        with self.config.edit(self.args.level) as conf:
            if self.args.unset:
                self._check(conf, section, opt)
                del conf[section][opt]
            else:
                self._check(conf, section)
                conf[section][opt] = self.args.value

        if self.args.name == "cache.type":
            logger.warning(
                "You have changed the 'cache.type' option. This doesn't update"
                " any existing workspace file links, but it can be done with:"
                "\n             dvc checkout --relink"
            )

        return 0

    def _check(self, conf, section, opt=None):
        if section not in conf:
            msg = "section {} doesn't exist"
            raise ConfigError(msg.format(self.args.name))

        if opt and opt not in conf[section]:
            msg = "option {} doesn't exist"
            raise ConfigError(msg.format(self.args.name))


parent_config_parser = argparse.ArgumentParser(add_help=False)
level_group = parent_config_parser.add_mutually_exclusive_group()
level_group.add_argument(
    "--global",
    dest="level",
    action="store_const",
    const="global",
    help="Use global config.",
)
level_group.add_argument(
    "--system",
    dest="level",
    action="store_const",
    const="system",
    help="Use system config.",
)
level_group.add_argument(
    "--local",
    dest="level",
    action="store_const",
    const="local",
    help="Use local config.",
)
parent_config_parser.set_defaults(level="repo")


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
