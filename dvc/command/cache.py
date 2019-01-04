from dvc.command.remote import CmdRemoteAdd
from dvc.command.config import CmdConfig


class CmdCacheDir(CmdConfig):
    def run(self):
        self.args.name = 'cache.dir'
        self.args.value = CmdRemoteAdd.resolve_path(self.args.value,
                                                    self.configobj.filename)

        return super(CmdCacheDir, self).run()
