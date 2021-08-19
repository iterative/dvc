import argparse

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.config import CmdConfig
from dvc.config import ConfigError
from dvc.ui import ui
from dvc.utils import format_link


class CmdExecutorConfig(CmdConfig):
    def __init__(self, args):

        super().__init__(args)
        if not self.config["feature"].get("executor", False):
            raise ConfigError("executor feature is disabled")

        if getattr(self.args, "name", None):
            self.args.name = self.args.name.lower()

    def _check_exists(self, conf):
        if self.args.name not in conf["executor"]:
            raise ConfigError(f"executor '{self.args.name}' doesn't exist.")


class CmdExecutorAdd(CmdExecutorConfig):
    def run(self):
        if self.args.default:
            ui.write(f"Setting '{self.args.name}' as a default executor.")

        with self.config.edit(self.args.level) as conf:
            if self.args.name in conf["executor"] and not self.args.force:
                raise ConfigError(
                    "executor '{}' already exists. Use `-f|--force` to "
                    "overwrite it.".format(self.args.name)
                )

            conf["executor"][self.args.name] = {"cloud": self.args.cloud}
            if self.args.default:
                conf["core"]["executor"] = self.args.name

        return 0


class CmdExecutorRemove(CmdExecutorConfig):
    def run(self):
        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            del conf["executor"][self.args.name]

        up_to_level = self.args.level or "repo"
        # Remove core.executor refs to this executor in any shadowing configs
        for level in reversed(self.config.LEVELS):
            with self.config.edit(level) as conf:
                if conf["core"].get("executor") == self.args.name:
                    del conf["core"]["executor"]

            if level == up_to_level:
                break

        return 0


class CmdExecutorModify(CmdExecutorConfig):
    def run(self):
        from dvc.config import merge

        with self.config.edit(self.args.level) as conf:
            merged = self.config.load_config_to_level(self.args.level)
            merge(merged, conf)
            self._check_exists(merged)

            if self.args.name not in conf["executor"]:
                conf["executor"][self.args.name] = {}
            section = conf["executor"][self.args.name]
            if self.args.unset:
                section.pop(self.args.option, None)
            else:
                section[self.args.option] = self.args.value
        return 0


class CmdExecutorDefault(CmdExecutorConfig):
    def run(self):
        if self.args.name is None and not self.args.unset:
            conf = self.config.read(self.args.level)
            try:
                print(conf["core"]["executor"])
            except KeyError:
                ui.write("No default executor set")
                return 1
        else:
            with self.config.edit(self.args.level) as conf:
                if self.args.unset:
                    conf["core"].pop("executor", None)
                else:
                    merged_conf = self.config.load_config_to_level(
                        self.args.level
                    )
                    if (
                        self.args.name in conf["executor"]
                        or self.args.name in merged_conf["executor"]
                    ):
                        conf["core"]["executor"] = self.args.name
                    else:
                        raise ConfigError(
                            "default executor must be present in executor "
                            "list."
                        )
        return 0


class CmdExecutorInit(CmdBase):
    def run(self):
        self.repo.executor.init(self.args.name)


class CmdExecutorDestroy(CmdBase):
    def run(self):
        self.repo.executor.destroy(self.args.name)


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    executor_HELP = "Set up and manage cloud executors."
    executor_parser = subparsers.add_parser(
        "executor",
        parents=[parent_parser],
        description=append_doc_link(executor_HELP, "executor"),
        # NOTE: suppress help during development to hide command
        # help=executor_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    executor_subparsers = executor_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc executor CMD --help` for " "command-specific help.",
    )

    fix_subparsers(executor_subparsers)

    executor_ADD_HELP = "Add a new data executor."
    executor_add_parser = executor_subparsers.add_parser(
        "add",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_ADD_HELP, "executor/add"),
        help=executor_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_add_parser.add_argument("name", help="Name of the executor")
    executor_add_parser.add_argument(
        "cloud",
        help="Executor cloud. See full list of supported clouds at {}".format(
            format_link(
                "https://github.com/iterative/"
                "terraform-provider-iterative#machine"
            )
        ),
    )
    executor_add_parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        default=False,
        help="Set as default executor.",
    )
    executor_add_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force overwriting existing configs",
    )
    executor_add_parser.set_defaults(func=CmdExecutorAdd)

    executor_DEFAULT_HELP = "Set/unset the default executor."
    executor_default_parser = executor_subparsers.add_parser(
        "default",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_DEFAULT_HELP, "executor/default"),
        help=executor_DEFAULT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_default_parser.add_argument(
        "name", nargs="?", help="Name of the executor"
    )
    executor_default_parser.add_argument(
        "-u",
        "--unset",
        action="store_true",
        default=False,
        help="Unset default executor.",
    )
    executor_default_parser.set_defaults(func=CmdExecutorDefault)

    executor_MODIFY_HELP = "Modify the configuration of an executor."
    executor_modify_parser = executor_subparsers.add_parser(
        "modify",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_MODIFY_HELP, "executor/modify"),
        help=executor_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_modify_parser.add_argument("name", help="Name of the executor")
    executor_modify_parser.add_argument(
        "option", help="Name of the option to modify."
    )
    executor_modify_parser.add_argument(
        "value", nargs="?", help="(optional) Value of the option."
    )
    executor_modify_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    executor_modify_parser.set_defaults(func=CmdExecutorModify)

    executor_REMOVE_HELP = "Remove an executor."
    executor_remove_parser = executor_subparsers.add_parser(
        "remove",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_REMOVE_HELP, "executor/remove"),
        help=executor_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_remove_parser.add_argument(
        "name", help="Name of the executor to remove."
    )
    executor_remove_parser.set_defaults(func=CmdExecutorRemove)

    executor_INIT_HELP = "Initialize an executor instance."
    executor_init_parser = executor_subparsers.add_parser(
        "init",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_INIT_HELP, "executor/init"),
        help=executor_INIT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_init_parser.add_argument(
        "name", help="Name of the executor to initialize."
    )
    executor_init_parser.set_defaults(func=CmdExecutorInit)

    executor_DESTROY_HELP = "Destroy an executor instance."
    executor_destroy_parser = executor_subparsers.add_parser(
        "destroy",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(executor_DESTROY_HELP, "executor/destroy"),
        help=executor_DESTROY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    executor_destroy_parser.add_argument(
        "name", help="Name of the executor instance to destroy."
    )
    executor_destroy_parser.set_defaults(func=CmdExecutorDestroy)
