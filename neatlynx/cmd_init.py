import os
import sys
from pathlib import Path

from neatlynx.cmd_base import CmdBase
from neatlynx.logger import Logger
from neatlynx.config import Config
from neatlynx.exceptions import NeatLynxException


class InitError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Init error: {}'.format(msg))


class CmdInit(CmdBase):
    CONFIG_TEMPLATE = '''[Global]
DataDir = {}
CacheDir = {}
StateDir = {}
Cloud = AWS

[AWS]
AccessKeyId =
SecretAccessKey =

StoragePath = neatlynx/tutorial


Region = us-east-1
Zone = us-east-1a

Image = ami-2d39803a

InstanceType = t2.nano

SpotPrice =
SpotTimeout = 300

Storage = my-100gb-drive-io

KeyDir = ~/.ssh
KeyName = neatlynx-key

SecurityGroup = neatlynx-group'''

    def __init__(self):
        CmdBase.__init__(self, parse_config=False)
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, '--data-dir',  'NeatLynx data directory', 'data')
        self.add_string_arg(parser, '--cache-dir', 'NeatLynx cache directory', 'cache')
        self.add_string_arg(parser, '--state-dir', 'NeatLynx state directory', 'state')
        pass

    def get_not_existing_dir(self, dir):
        path = Path(os.path.join(self.git.git_dir, dir))
        if path.exists():
            raise InitError('Directory "{}" already exist'.format(path.name))
        return path

    def get_not_existing_conf_file(self):
        path = Path(os.path.join(self.git.git_dir, Config.CONFIG))
        if path.exists():
            raise InitError('Configuration file "{}" already exist'.format(path.name))
        return path

    def run(self):
        data_dir_path = self.get_not_existing_dir(self.args.data_dir)
        cache_dir_path = self.get_not_existing_dir(self.args.cache_dir)
        state_dir_path = self.get_not_existing_dir(self.args.state_dir)

        conf = self.get_not_existing_conf_file()

        data_dir_path.mkdir()
        cache_dir_path.mkdir()
        state_dir_path.mkdir()
        Logger.info('Directories {}, {} and {} were created'.format(
            data_dir_path.name,
            cache_dir_path.name,
            state_dir_path.name))

        conf.write_text(self.CONFIG_TEMPLATE.format(data_dir_path.name,
                                                    cache_dir_path.name,
                                                    state_dir_path.name))

        self.modify_gitignore(cache_dir_path.name)
        pass

    def modify_gitignore(self, cache_dir_name):
        gitignore_file = os.path.join(self.git.git_dir, '.gitignore')
        if not os.path.exists(gitignore_file):
            open(gitignore_file, 'a').close()
            Logger.info('File .gitignore was created')
        with open(gitignore_file, 'a') as fd:
            fd.write('\ncache')
        Logger.info('Directory {} was added to .gitignore file'.format(cache_dir_name))


if __name__ == '__main__':
    try:
        CmdInit().run()
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
