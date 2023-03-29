import argparse
import logging
import os

from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)

NAME_REGEX = r"^(?P<remote>remote\.)?(?P<section>[^\.]*)\.(?P<option>[^\.]*)$"


def _name_type(value):
    import re

    match = re.match(NAME_REGEX, value)
    if not match:
        raise argparse.ArgumentTypeError(
            "name argument should look like remote.name.option or section.option"
        )
    return (
        bool(match.group("remote")),
        match.group("section").lower(),
        match.group("option").lower(),
    )


class CmdConfig(CmdBaseNoRepo):
    def __init__(self, args):
        from dvc.config import Config

        super().__init__(args)

        self.config = Config.from_cwd(validate=False)

    def run(self):
        if self.args.show_origin and (self.args.value or self.args.unset):
            logger.error(
                "--show-origin can't be used together with any of these "
                "options: -u/--unset, value"
            )
            return 1

        if self.args.list:
            return self._list()

        if self.args.name is None:
            logger.error("name argument is required")
            return 1

        remote, section, opt = self.args.name

        if self.args.value is None and not self.args.unset:
            return self._get(remote, section, opt)

        return self._set(remote, section, opt)

    def _list(self):
        if any((self.args.name, self.args.value, self.args.unset)):
            logger.error(
                "-l/--list can't be used together with any of these "
                "options: -u/--unset, name, value"
            )
            return 1

        levels = self._get_appropriate_levels(self.args.level)

        for level in levels:
            conf = self.config.read(level)
            prefix = self._config_file_prefix(self.args.show_origin, self.config, level)
            configs = list(self._format_config(conf, prefix))
            if configs:
                ui.write("\n".join(configs))

        return 0

    def _get(self, remote, section, opt):
        from dvc.config import ConfigError

        levels = self._get_appropriate_levels(self.args.level)[::-1]

        for level in levels:
            conf = self.config.read(level)
            if remote:
                conf = conf["remote"]

            try:
                self._check(conf, remote, section, opt)
            except ConfigError:
                if self.args.level:
                    raise
            else:
                prefix = self._config_file_prefix(
                    self.args.show_origin, self.config, level
                )
                ui.write(prefix, conf[section][opt], sep="")
                break

        return 0

    def _set(self, remote, section, opt):
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
        from dvc.config import ConfigError

        name = "remote" if remote else "section"
        if section not in conf:
            raise ConfigError(f"{name} '{section}' doesn't exist")

        if opt and opt not in conf[section]:
            raise ConfigError(f"option '{opt}' doesn't exist in {name} '{section}'")

    def _get_appropriate_levels(self, levels):
        if levels:
            self._validate_level_for_non_repo_operation(levels)
            return [levels]
        if self.config.dvc_dir is None:
            return self.config.SYSTEM_LEVELS
        return self.config.LEVELS

    def _validate_level_for_non_repo_operation(self, level):
        from dvc.config import ConfigError

        if self.config.dvc_dir is None and level in self.config.REPO_LEVELS:
            raise ConfigError("Not inside a DVC repo")

    @staticmethod
    def _format_config(config, prefix=""):
        from dvc.utils.flatten import flatten

        for key, value in flatten(config).items():
            yield f"{prefix}{key}={value}"

    @staticmethod
    def _config_file_prefix(show_origin, config, level):
        from dvc.repo import Repo

        if not show_origin:
            return ""

        level = level or "repo"
        fname = config.files[level]

        if level in ["local", "repo"]:
            fname = os.path.relpath(fname, start=Repo.find_root())

        return fname + "\t"


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
    "--project",
    dest="level",
    action="store_const",
    const="repo",
    help="Use project config (.dvc/config).",
)
level_group.add_argument(
    "--local",
    dest="level",
    action="store_const",
    const="local",
    help="Use local config (.dvc/config.local).",
)
parent_config_parser.set_defaults(level=None)


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
        nargs="?",
        type=_name_type,
        help="Option name (section.option or remote.name.option).",
    )
    config_parser.add_argument("value", nargs="?", help="Option value.")
    config_parser.add_argument(
        "-l",
        "--list",
        default=False,
        action="store_true",
        help="List all defined config values.",
    )
    config_parser.add_argument(
        "--show-origin",
        default=False,
        action="store_true",
        help="Show the source file containing each config value.",
    )
    config_parser.set_defaults(func=CmdConfig)
