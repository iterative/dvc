import os
import sys
from pathlib import Path

from neatlynx.base_cmd import BaseCmd, Logger
from neatlynx.exceptions import NeatLynxException


class InitError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Init error: {}'.format(msg))


class CmdInit(BaseCmd):
    DEFAULT_CONFIG = 'neatlynx.conf.default'

    CONFIG_TEMPLATE = '''[Global]
DataDir = {}
StateDir = {}
CacheDir = {}
Cloud = AWS

[AWS]
AccessKeyID =
SecretAccessKey =
Region = us-east-1
Zone = us-east-1a

StorageBucket = neatlynx-tutorial
Image = ami-2d39803a

InstanceType = t2.nano

SpotPrice =
SpotTimeout = 300

Storage = my-100gb-drive-io

KeyDir = ~/.ssh
KeyName = neatlynx-key

SecurityGroup = neatlynx-group'''

    def __init__(self):
        BaseCmd.__init__(self, parse_config=False)
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, '--data-dir',  'data',  'NeatLynx data directory')
        self.add_string_arg(parser, '--cache-dir', 'cache', 'NeatLynx cache directory')
        self.add_string_arg(parser, '--state-dir', 'state', 'NeatLynx state directory')
        pass

    def get_not_existing_dir(self, dir):
        path = Path(os.path.join(self.git_dir, dir))
        if path.exists():
            raise InitError('Directory "{}" already exist'.format(path.name))
        return path

    def get_not_existing_conf_file(self):
        path = Path(os.path.join(self.git_dir, self.CONFIG))
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

        conf.write_text(self.CONFIG_TEMPLATE.format(data_dir_path.name,
                                                    cache_dir_path.name,
                                                    state_dir_path.name))
        pass

if __name__ == '__main__':
    try:
        CmdInit().run()
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
