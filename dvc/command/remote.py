from dvc.command.common.base import CmdBase
from dvc.config import Config
from dvc.command.config import CmdConfig


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
