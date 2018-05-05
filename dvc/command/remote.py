import re

from dvc.config import Config
from dvc.command.config import CmdConfig
from dvc.logger import Logger


class CmdRemoteAdd(CmdConfig):
    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        return self.set(section, Config.SECTION_REMOTE_URL, self.args.url)


class CmdRemoteRemove(CmdConfig):
    def run(self):
        section = Config.SECTION_REMOTE_FMT.format(self.args.name)
        return self.unset(section, Config.SECTION_REMOTE_URL)


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
                url = self.configobj[section].get(Config.SECTION_REMOTE_URL, '')
                Logger.info('{}\t{}'.format(name, url))
        return 0
