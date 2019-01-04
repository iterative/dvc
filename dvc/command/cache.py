import os

from dvc.command.config import CmdConfig


class CmdCacheDir(CmdConfig):
    def run(self):
        self.args.name = 'cache.dir'

        path = self.args.value
        if not os.path.isabs(path):
            config_dir = os.path.dirname(self.configobj.filename)
            path = os.path.relpath(path, config_dir)
        self.args.value = path

        return super(CmdCacheDir, self).run()
