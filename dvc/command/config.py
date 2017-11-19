import os
import configobj

from dvc.command.common.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.config import Config


class CmdConfig(CmdBase):
    def __init__(self, settings):
        super(CmdConfig, self).__init__(settings)

        # Using configobj because it doesn't
        # drop comments like configparser does.
        self.configobj = configobj.ConfigObj(self._config_path, write_empty_values=True)

    @property
    def _config_path(self):
        return os.path.join(self.git.git_dir, Config.CONFIG_DIR, Config.CONFIG)

    def _get_key(self, d, name, add=False):
        for k in d.keys():
            if k.lower() == name.lower():
                return k

        if add:
            d[name] = {}
            return name

        return None

    def unset(self):
        try:
            del self.configobj[self.section][self.opt]
            self.configobj.write()
        except Exception as exc:
            Logger.error('Failed to unset \'{}\': {}'.format(self.parsed_args.name, exc))
            return 1

        return 0

    def show(self):
        Logger.info(self.configobj[self.section][self.opt])
        return 0

    def set(self):
        try:
            self.configobj[self.section][self.opt] = self.parsed_args.value
            self.configobj.write()
        except Exception as exc:
            Logger.error('Failed to set \'{}\' to \'{}\': {}'.format(self.parsed_args.name,
                                                                     self.parsed_args.value,
                                                                     exc))
            return 1

        return 0

    def check_opt(self):
        _section, _opt = self.parsed_args.name.strip().split('.', 1)
        add = (self.parsed_args.value != None and self.parsed_args.unset == False)

        section = self._get_key(self.configobj, _section, add)

        if not section:
            Logger.error('Invalid option name {}'.format(_section))
            return 1

        opt = self._get_key(self.configobj[section], _opt, add)
        if not opt:
            Logger.error('Invalid option value: {}'.format(_opt))
            return 1

        self.section = section
        self.opt = opt

        return 0

    def _run(self):
        if self.check_opt() != 0:
            return 1

        if self.parsed_args.unset:
            return self.unset()

        if self.parsed_args.value is None:
            return self.show()

        return self.set()

    def run(self):
        with DvcLock(self.is_locker, self.git):
            return self._run()
