import argparse
import logging

from dvc.config import ConfigError
from dvc.command.base import append_doc_link, fix_subparsers
from dvc.command.config import CmdConfig
from dvc.utils import format_link

logger = logging.getLogger(__name__)


class CmdRemote(CmdConfig):
    def __init__(self, args):
        super().__init__(args)

        if getattr(self.args, "name", None):
            self.args.name = self.args.name.lower()

    def _check_exists(self, conf):
        if self.args.name not in conf["remote"]:
            raise ConfigError(
                "remote '{}' doesn't exists.".format(self.args.name)
            )


class CmdRemoteAdd(CmdRemote):
    def run(self):
        if self.args.default:
            logger.info(
                "Setting '{}' as a default remote.".format(self.args.name)
            )

        with self.config.edit(self.args.level) as conf:
            if self.args.name in conf["remote"] and not self.args.force:
                raise ConfigError(
                    "remote '{}' already exists. Use `-f|--force` to "
                    "overwrite it.".format(self.args.name)
                )

            conf["remote"][self.args.name] = {"url": self.args.url}
            if self.args.default:
                conf["core"]["remote"] = self.args.name

        return 0


class CmdRemoteRemove(CmdRemote):
    def run(self):
        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            del conf["remote"][self.args.name]

        # Remove core.remote refs to this remote in any shadowing configs
        for level in reversed(self.config.LEVELS):
            with self.config.edit(level) as conf:
                if conf["core"].get("remote") == self.args.name:
                    del conf["core"]["remote"]

            if level == self.args.level:
                break

        return 0


class CmdRemoteModify(CmdRemote):
    def run(self):
        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            conf["remote"][self.args.name][self.args.option] = self.args.value
        return 0


class CmdRemoteDefault(CmdRemote):
    def run(self):

        if self.args.name is None and not self.args.unset:
            conf = self.config.load_one(self.args.level)
            try:
                print(conf["core"]["remote"])
            except KeyError:
                logger.info("No default remote set")
                return 1
        else:
            with self.config.edit(self.args.level) as conf:
                if self.args.unset:
                    conf["core"].pop("remote", None)
                else:
                    conf["core"]["remote"] = self.args.name
        return 0


class CmdRemoteList(CmdRemote):
    def run(self):
        conf = self.config.load_one(self.args.level)
        for name, conf in conf["remote"].items():
            logger.info("{}\t{}".format(name, conf["url"]))
        return 0


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    REMOTE_HELP = "Manage remote storage configuration."
    remote_parser = subparsers.add_parser(
        "remote",
        parents=[parent_parser],
        description=append_doc_link(REMOTE_HELP, "remote"),
        help=REMOTE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    remote_subparsers = remote_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc remote CMD --help` for " "command-specific help.",
    )

    fix_subparsers(remote_subparsers)

    REMOTE_ADD_HELP = "Add remote."
    remote_add_parser = remote_subparsers.add_parser(
        "add",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_ADD_HELP, "remote/add"),
        help=REMOTE_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_add_parser.add_argument("name", help="Name of the remote")
    remote_add_parser.add_argument(
        "url",
        help="URL. See full list of supported urls at {}".format(
            format_link("https://man.dvc.org/remote")
        ),
    )
    remote_add_parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        default=False,
        help="Set as default remote.",
    )
    remote_add_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force overwriting existing configs",
    )
    remote_add_parser.set_defaults(func=CmdRemoteAdd)

    REMOTE_DEFAULT_HELP = "Set/unset default remote."
    remote_default_parser = remote_subparsers.add_parser(
        "default",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_DEFAULT_HELP, "remote/default"),
        help=REMOTE_DEFAULT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_default_parser.add_argument(
        "name", nargs="?", help="Name of the remote."
    )
    remote_default_parser.add_argument(
        "-u",
        "--unset",
        action="store_true",
        default=False,
        help="Unset default remote.",
    )
    remote_default_parser.set_defaults(func=CmdRemoteDefault)

    REMOTE_REMOVE_HELP = "Remove remote."
    remote_remove_parser = remote_subparsers.add_parser(
        "remove",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_REMOVE_HELP, "remote/remove"),
        help=REMOTE_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_remove_parser.add_argument(
        "name", help="Name of the remote to remove."
    )
    remote_remove_parser.set_defaults(func=CmdRemoteRemove)

    REMOTE_MODIFY_HELP = "Modify remote."
    remote_modify_parser = remote_subparsers.add_parser(
        "modify",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_MODIFY_HELP, "remote/modify"),
        help=REMOTE_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_modify_parser.add_argument("name", help="Name of the remote.")
    remote_modify_parser.add_argument(
        "option", help="Name of the option to modify."
    )
    remote_modify_parser.add_argument(
        "value", nargs="?", help="(optional) Value of the option."
    )
    remote_modify_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    remote_modify_parser.set_defaults(func=CmdRemoteModify)

    REMOTE_LIST_HELP = "List available remotes."
    remote_list_parser = remote_subparsers.add_parser(
        "list",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(REMOTE_LIST_HELP, "remote/list"),
        help=REMOTE_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remote_list_parser.set_defaults(func=CmdRemoteList)
