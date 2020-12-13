import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.config import Config, ConfigError

logger = logging.getLogger(__name__)

NAME_REGEX = r"^(?P<remote>remote\.)?(?P<section>[^\.]*)\.(?P<option>[^\.]*)$"


def _name_type(value):
    import re

    match = re.match(NAME_REGEX, value)
    if not match:
        raise argparse.ArgumentTypeError(
            "name argument should look like "
            "remote.name.option or "
            "section.option"
        )
    return (
        bool(match.group("remote")),
        match.group("section").lower(),
        match.group("option").lower(),
    )


class CmdConfig(CmdBaseNoRepo):
    def __init__(self, args):
        super().__init__(args)

        self.config = Config(validate=False)

    def run(self):
        remote, section, opt = self.args.name

        if self.args.value is None and not self.args.unset:
            conf = self.config.load_one(self.args.level)
            if remote:
                conf = conf["remote"]
            self._check(conf, remote, section, opt)
            logger.info(conf[section][opt])
            return 0

        with self.config.edit(self.args.level) as conf:
            if remote:
                conf = conf["remote"]
            if self.args.unset:
                self._check(conf, remote, section, opt)
                del conf[section][opt]
            else:
                self._check(conf, remote, section)
                conf[section][opt] = self.args.value

        if self.args.name == "cache.type":
            logger.warning(
                "You have changed the 'cache.type' option. This doesn't update"
                " any existing workspace file links, but it can be done with:"
                "\n             dvc checkout --relink"
            )

        return 0

    def _check(self, conf, remote, section, opt=None):
        name = "remote" if remote else "section"
        if section not in conf:
            raise ConfigError(f"{name} '{section}' doesn't exist")

        if opt and opt not in conf[section]:
            raise ConfigError(
                f"option '{opt}' doesn't exist in {name} '{section}'"
            )


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
    config_parser.add_argument(
        "name",
        type=_name_type,
        help="Option name (remote.name.option or section.option).",
    )
    config_parser.add_argument("value", nargs="?", help="Option value.")
    config_parser.set_defaults(func=CmdConfig)
