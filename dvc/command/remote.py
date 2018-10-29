import re

from dvc.config import Config
from dvc.command.config import CmdConfig
from dvc.logger import Logger


class CmdRemoteAdd(CmdConfig):
    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        ret = self._set(section, Config.SECTION_REMOTE_URL, self.args.url)
        if ret != 0:
            return ret

        if self.args.default:
            msg = 'Setting \'{}\' as a default remote.'.format(self.args.name)
            Logger.info(msg)
            ret = self._set(Config.SECTION_CORE,
                            Config.SECTION_CORE_REMOTE,
                            self.args.name)

        return ret


class CmdRemoteRemove(CmdConfig):
    def _remove_default(self, config):
        core = config.get(Config.SECTION_CORE, None)
        if core is None:
            return 0

        default = core.get(Config.SECTION_CORE_REMOTE, None)
        if default is None:
            return 0

        if default == self.args.name:
            return self._unset(Config.SECTION_CORE,
                               opt=Config.SECTION_CORE_REMOTE,
                               configobj=config)

    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        ret = self._unset(section)
        if ret != 0:
            return ret

        for configobj in [self.config._local_config,
                          self.config._project_config,
                          self.config._global_config,
                          self.config._system_config]:
            self._remove_default(configobj)
            self.config.save(configobj)
            if configobj == self.configobj:
                break

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
