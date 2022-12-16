import argparse

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link, fix_subparsers
from dvc.commands.config import CmdConfig
from dvc.compare import TabularData
from dvc.config import ConfigError
from dvc.exceptions import DvcException
from dvc.types import Dict, List
from dvc.ui import ui
from dvc.utils import format_link


class MachineDisabledError(ConfigError):
    def __init__(self):
        super().__init__("Machine feature is disabled")


class CmdMachineConfig(CmdConfig):
    def __init__(self, args):

        super().__init__(args)
        if not self.config["feature"].get("machine", False):
            raise MachineDisabledError

        if getattr(self.args, "name", None):
            self.args.name = self.args.name.lower()

    def _check_exists(self, conf):
        if self.args.name not in conf["machine"]:
            raise ConfigError(f"machine '{self.args.name}' doesn't exist.")


class CmdMachineAdd(CmdMachineConfig):
    def run(self):
        from dvc.machine import validate_name

        validate_name(self.args.name)

        if self.args.default:
            ui.write(f"Setting '{self.args.name}' as a default machine.")

        with self.config.edit(self.args.level) as conf:
            if self.args.name in conf["machine"] and not self.args.force:
                raise ConfigError(
                    "machine '{}' already exists. Use `-f|--force` to "
                    "overwrite it.".format(self.args.name)
                )

            conf["machine"][self.args.name] = {"cloud": self.args.cloud}
            if self.args.default:
                conf["core"]["machine"] = self.args.name

        return 0


class CmdMachineRemove(CmdMachineConfig):
    def run(self):
        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            del conf["machine"][self.args.name]

        up_to_level = self.args.level or "repo"
        # Remove core.machine refs to this machine in any shadowing configs
        for level in reversed(self.config.LEVELS):
            with self.config.edit(level) as conf:
                if conf["core"].get("machine") == self.args.name:
                    del conf["core"]["machine"]

            if level == up_to_level:
                break

        return 0


class CmdMachineList(CmdMachineConfig):
    TABLE_COLUMNS = [
        "name",
        "cloud",
        "region",
        "image",
        "spot",
        "spot_price",
        "instance_hdd_size",
        "instance_type",
        "ssh_private",
        "startup_script",
    ]

    PRIVATE_COLUMNS = ["ssh_private", "startup_script"]

    def _hide_private(self, conf):
        for machine in conf:
            for column in self.PRIVATE_COLUMNS:
                if column in conf[machine]:
                    conf[machine][column] = "***"

    def _show_origin(self):
        levels = [self.args.level] if self.args.level else self.config.LEVELS
        for level in levels:
            conf = self.config.read(level)["machine"]
            if self.args.name:
                conf = conf.get(self.args.name, {})
            self._hide_private(conf)
            prefix = self._config_file_prefix(True, self.config, level)
            configs = list(self._format_config(conf, prefix))
            if configs:
                ui.write("\n".join(configs))

    def _show_table(self):
        td = TabularData(self.TABLE_COLUMNS, fill_value="-")
        conf = self.config.read()["machine"]
        if self.args.name:
            conf = {self.args.name: conf.get(self.args.name, {})}
        self._hide_private(conf)
        for machine, machine_config in conf.items():
            machine_config["name"] = machine
            td.row_from_dict(machine_config)
        td.dropna("cols", "all")
        td.render()

    def run(self):
        if self.args.show_origin:
            self._show_origin()
        else:
            self._show_table()
        return 0


class CmdMachineModify(CmdMachineConfig):
    def run(self):
        from dvc.config import merge

        with self.config.edit(self.args.level) as conf:
            merged = self.config.load_config_to_level(self.args.level)
            merge(merged, conf)
            self._check_exists(merged)

            if self.args.name not in conf["machine"]:
                conf["machine"][self.args.name] = {}
            section = conf["machine"][self.args.name]
            if self.args.unset:
                section.pop(self.args.option, None)
            else:
                section[self.args.option] = self.args.value
        return 0


class CmdMachineRename(CmdBase):
    def _check_exists(self, conf):
        if self.args.name not in conf["machine"]:
            raise ConfigError(f"machine '{self.args.name}' doesn't exist.")

    def _rename_default(self, conf):
        if conf["core"].get("machine") == self.args.name:
            conf["core"]["machine"] = self.args.new

    def _check_before_rename(self):
        from dvc.machine import validate_name

        validate_name(self.args.new)

        all_config = self.config.load_config_to_level(None)
        if self.args.new in all_config.get("machine", {}):
            raise ConfigError(
                "Rename failed. Machine '{}' already exists.".format(
                    self.args.new
                )
            )
        ui.write(f"Rename machine '{self.args.name}' to '{self.args.new}'.")

    def run(self):

        self._check_before_rename()

        with self.config.edit(self.args.level) as conf:
            self._check_exists(conf)
            conf["machine"][self.args.new] = conf["machine"][self.args.name]
            try:
                self.repo.machine.rename(self.args.name, self.args.new)
            except DvcException as error:
                del conf["machine"][self.args.new]
                raise ConfigError("terraform rename failed") from error
            del conf["machine"][self.args.name]
            self._rename_default(conf)

        up_to_level = self.args.level or "repo"
        for level in reversed(self.config.LEVELS):
            if level == up_to_level:
                break
            with self.config.edit(level) as level_conf:
                self._rename_default(level_conf)

        return 0


class CmdMachineDefault(CmdMachineConfig):
    def run(self):
        if self.args.name is None and not self.args.unset:
            conf = self.config.read(self.args.level)
            try:
                print(conf["core"]["machine"])
            except KeyError:
                ui.write("No default machine set")
                return 1
        else:
            with self.config.edit(self.args.level) as conf:
                if self.args.unset:
                    conf["core"].pop("machine", None)
                else:
                    merged_conf = self.config.load_config_to_level(
                        self.args.level
                    )
                    if (
                        self.args.name in conf["machine"]
                        or self.args.name in merged_conf["machine"]
                    ):
                        conf["core"]["machine"] = self.args.name
                    else:
                        raise ConfigError(
                            "default machine must be present in machine "
                            "list."
                        )
        return 0


class CmdMachineCreate(CmdBase):
    def run(self):
        if self.repo.machine is None:
            raise MachineDisabledError

        self.repo.machine.create(self.args.name)
        return 0


class CmdMachineStatus(CmdBase):
    INSTANCE_FIELD = ["name", "instance", "status"]
    SHOWN_FIELD = [
        "cloud",
        "instance_ip",
        "instance_type",
        "instance_hdd_size",
        "instance_gpu",
    ]
    FILL_VALUE = "-"

    def _add_row(
        self,
        name: str,
        all_status: List[Dict],
        td: TabularData,
    ):

        if not all_status:
            row = [
                name,
                self.FILL_VALUE,
                "offline",
            ]  # back to `None` after #7167
            td.append(row)
        for i, status in enumerate(all_status, start=1):
            row = [name, f"num_{i}", "running" if status else "offline"]
            for field in self.SHOWN_FIELD:
                value = str(status.get(field, ""))
                row.append(value)
            td.append(row)

    def run(self):
        if self.repo.machine is None:
            raise MachineDisabledError

        td = TabularData(
            self.INSTANCE_FIELD + self.SHOWN_FIELD, fill_value=self.FILL_VALUE
        )

        if self.args.name:
            all_status = list(self.repo.machine.status(self.args.name))
            self._add_row(self.args.name, all_status, td)
        else:
            name_set = set()
            for level in self.repo.config.LEVELS:
                conf = self.repo.config.read(level)["machine"]
                name_set.update(conf.keys())
            name_list = list(name_set)
            for name in sorted(name_list):
                all_status = list(self.repo.machine.status(name))
                self._add_row(name, all_status, td)

        td.dropna("cols", "all")
        td.render()
        return 0


class CmdMachineDestroy(CmdBase):
    def run(self):
        if self.repo.machine is None:
            raise MachineDisabledError

        self.repo.machine.destroy(self.args.name)
        return 0


class CmdMachineSsh(CmdBase):
    def run(self):
        if self.repo.machine is None:
            raise MachineDisabledError

        self.repo.machine.run_shell(self.args.name)
        return 0


def add_parser(subparsers, parent_parser):
    from dvc.commands.config import parent_config_parser

    machine_HELP = "Set up and manage cloud machines."
    machine_parser = subparsers.add_parser(
        "machine",
        parents=[parent_parser],
        description=append_doc_link(machine_HELP, "machine"),
        # NOTE: suppress help during development to hide command
        # help=machine_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    machine_subparsers = machine_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc machine CMD --help` for " "command-specific help.",
    )

    fix_subparsers(machine_subparsers)

    machine_ADD_HELP = "Add a new data machine."
    machine_add_parser = machine_subparsers.add_parser(
        "add",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_ADD_HELP, "machine/add"),
        help=machine_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_add_parser.add_argument("name", help="Name of the machine")
    machine_add_parser.add_argument(
        "cloud",
        help="Machine cloud. See full list of supported clouds at {}".format(
            format_link(
                "https://github.com/iterative/"
                "terraform-provider-iterative#machine"
            )
        ),
    )
    machine_add_parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        default=False,
        help="Set as default machine.",
    )
    machine_add_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force overwriting existing configs",
    )
    machine_add_parser.set_defaults(func=CmdMachineAdd)

    machine_DEFAULT_HELP = "Set/unset the default machine."
    machine_default_parser = machine_subparsers.add_parser(
        "default",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_DEFAULT_HELP, "machine/default"),
        help=machine_DEFAULT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_default_parser.add_argument(
        "name", nargs="?", help="Name of the machine"
    )
    machine_default_parser.add_argument(
        "-u",
        "--unset",
        action="store_true",
        default=False,
        help="Unset default machine.",
    )
    machine_default_parser.set_defaults(func=CmdMachineDefault)

    machine_LIST_HELP = "List the configuration of one/all machines."
    machine_list_parser = machine_subparsers.add_parser(
        "list",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_LIST_HELP, "machine/list"),
        help=machine_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_list_parser.add_argument(
        "--show-origin",
        default=False,
        action="store_true",
        help="Show the source file containing each config value.",
    )
    machine_list_parser.add_argument(
        "name",
        nargs="?",
        type=str,
        help="name of machine to specify",
    )
    machine_list_parser.set_defaults(func=CmdMachineList)
    machine_MODIFY_HELP = "Modify the configuration of an machine."
    machine_modify_parser = machine_subparsers.add_parser(
        "modify",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_MODIFY_HELP, "machine/modify"),
        help=machine_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_modify_parser.add_argument("name", help="Name of the machine")
    machine_modify_parser.add_argument(
        "option", help="Name of the option to modify."
    )
    machine_modify_parser.add_argument(
        "value", nargs="?", help="(optional) Value of the option."
    )
    machine_modify_parser.add_argument(
        "-u",
        "--unset",
        default=False,
        action="store_true",
        help="Unset option.",
    )
    machine_modify_parser.set_defaults(func=CmdMachineModify)

    machine_RENAME_HELP = "Rename a machine "
    machine_rename_parser = machine_subparsers.add_parser(
        "rename",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_RENAME_HELP, "remote/rename"),
        help=machine_RENAME_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_rename_parser.add_argument("name", help="Machine to be renamed")
    machine_rename_parser.add_argument("new", help="New name of the machine")
    machine_rename_parser.set_defaults(func=CmdMachineRename)

    machine_REMOVE_HELP = "Remove an machine."
    machine_remove_parser = machine_subparsers.add_parser(
        "remove",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(machine_REMOVE_HELP, "machine/remove"),
        help=machine_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_remove_parser.add_argument(
        "name", help="Name of the machine to remove."
    )
    machine_remove_parser.set_defaults(func=CmdMachineRemove)

    machine_CREATE_HELP = "Create and start a machine instance."
    machine_create_parser = machine_subparsers.add_parser(
        "create",
        parents=[parent_parser],
        description=append_doc_link(machine_CREATE_HELP, "machine/create"),
        help=machine_CREATE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_create_parser.add_argument(
        "name", help="Name of the machine to create."
    )
    machine_create_parser.set_defaults(func=CmdMachineCreate)

    machine_STATUS_HELP = (
        "List the status of running instances for one/all machines."
    )
    machine_status_parser = machine_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(machine_STATUS_HELP, "machine/status"),
        help=machine_STATUS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_status_parser.add_argument(
        "name", nargs="?", help="(optional) Name of the machine."
    )
    machine_status_parser.set_defaults(func=CmdMachineStatus)

    machine_DESTROY_HELP = "Destroy an machine instance."
    machine_destroy_parser = machine_subparsers.add_parser(
        "destroy",
        parents=[parent_parser],
        description=append_doc_link(machine_DESTROY_HELP, "machine/destroy"),
        help=machine_DESTROY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_destroy_parser.add_argument(
        "name", help="Name of the machine instance to destroy."
    )
    machine_destroy_parser.set_defaults(func=CmdMachineDestroy)

    machine_SSH_HELP = "Connect to a machine via SSH."
    machine_ssh_parser = machine_subparsers.add_parser(
        "ssh",
        parents=[parent_parser],
        description=append_doc_link(machine_SSH_HELP, "machine/ssh"),
        help=machine_SSH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    machine_ssh_parser.add_argument(
        "name", help="Name of the machine instance to connect to."
    )
    machine_ssh_parser.set_defaults(func=CmdMachineSsh)
