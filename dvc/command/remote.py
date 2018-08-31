import os
import re
import configobj

from dvc.config import Config
from dvc.command.config import CmdConfig
from dvc.logger import Logger


class CmdRemoteAdd(CmdConfig):
    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        ret = self.set(section, Config.SECTION_REMOTE_URL, self.args.url)
        if ret != 0:
            return ret

        if self.args.default:
            msg = 'Setting \'{}\' as a default remote.'.format(self.args.name)
            Logger.info(msg)
            ret = self.set(Config.SECTION_CORE,
                           Config.SECTION_CORE_REMOTE,
                           self.args.name)

        return ret


class CmdRemoteRemove(CmdConfig):
    def _remove_default(self, config_file, remote):
        path = os.path.join(os.path.dirname(self.config_file),
                            config_file)
        config = configobj.ConfigObj(path)

        core = config.get(Config.SECTION_CORE, None)
        if core is None:
            return

        default = core.get(Config.SECTION_CORE_REMOTE, None)
        if default is None:
            return

        if default == remote:
            del config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE]
            if len(config[Config.SECTION_CORE]) == 0:
                del config[Config.SECTION_CORE]

        config.write()

    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        ret = self.unset(section)
        if ret != 0:
            return ret

        self._remove_default(Config.CONFIG, self.args.name)
        self._remove_default(Config.CONFIG_LOCAL, self.args.name)
        return 0


class CmdRemoteModify(CmdConfig):
    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        self.args.name = '{}.{}'.format(section, self.args.option)
        return super(CmdRemoteModify, self).run()


class CmdRemoteList(CmdConfig):
    def run(self):
        for section in self.configobj.keys():
            r = re.match(Config.SECTION_REMOTE_REGEX, section)
            if r:
                name = r.group('name')
                url = self.configobj[section].get(Config.SECTION_REMOTE_URL,
                                                  '')
                Logger.info('{}\t{}'.format(name, url))
        return 0
