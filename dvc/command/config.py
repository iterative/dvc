import os
import configobj

from dvc.command.base import CmdBase
from dvc.logger import Logger
from dvc.config import Config

class CmdConfig(CmdBase):
    def __init__(self, settings):
        super(CmdConfig, self).__init__(settings)

    @property
    def _config_path(self):
        return os.path.join(self.git.git_dir, Config.CONFIG)

    def _find_key(self, l, name):
        for k in l:
            if k.lower() == name.lower():
                return k

        return None

    def run(self):
        _section, _opt = self.parsed_args.name.strip().split('.')

        # Using configobj because it doesn't
        # drop comments like configparser does.
        config = configobj.ConfigObj(self._config_path)

        section = self._find_key(config.keys(), _section)
        if not section:
            return 1

        opt = self._find_key(config[section].keys(), _opt)
        if not opt:
            return 1

        if self.parsed_args.value == None:
            Logger.info(config[section][opt])
            return 0

        try:
            config[section][opt] = self.parsed_args.value
            config.write()
        except Exception as exc:
            Logger.error('Failed to set \'{}\' to \'{}\': {}'.format(self.parsed_args.name,
                                                                     self.parsed_args.value,
                                                                     exc))
            return 1

        return 0
