import os
from pathlib import Path

from dvc.cmd_base import CmdBase
from dvc.logger import Logger
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.utils import run


class InitError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Init error: {}'.format(msg))


class CmdInit(CmdBase):
    CONFIG_TEMPLATE = '''[Global]
DataDir = {}
CacheDir = {}
StateDir = {}
Cloud = AWS

[AWS]
AccessKeyId =
SecretAccessKey =

StoragePath = dvc/tutorial


Region = us-east-1
Zone = us-east-1a

Image = ami-2d39803a

InstanceType = t2.nano

SpotPrice =
SpotTimeout = 300

Storage = my-100gb-drive-io

KeyDir = ~/.ssh
KeyName = dvc-key

SecurityGroup = dvc-group'''

    def __init__(self):
        CmdBase.__init__(self, parse_config=False)
        pass

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        self.add_string_arg(parser, '--data-dir',  'NeatLynx data directory', 'data')
        self.add_string_arg(parser, '--cache-dir', 'NeatLynx cache directory', 'cache')
        self.add_string_arg(parser, '--state-dir', 'NeatLynx state directory', 'state')
        pass

    def get_not_existing_dir(self, dir):
        path = Path(os.path.join(self.git.git_dir, dir))
        if path.exists():
            raise InitError('Directory "{}" already exist'.format(path.name))
        return path

    def get_not_existing_conf_file_name(self):
        file_name = os.path.join(self.git.git_dir, Config.CONFIG)
        if os.path.exists(file_name):
            raise InitError('Configuration file "{}" already exist'.format(file_name))
        return file_name

    def run(self):
        data_dir_path = self.get_not_existing_dir(self.args.data_dir)
        cache_dir_path = self.get_not_existing_dir(self.args.cache_dir)
        state_dir_path = self.get_not_existing_dir(self.args.state_dir)

        conf_file_name = self.get_not_existing_conf_file_name()

        data_dir_path.mkdir()
        cache_dir_path.mkdir()
        state_dir_path.mkdir()
        Logger.printing('Directories {}, {} and {} were created'.format(
            data_dir_path.name,
            cache_dir_path.name,
            state_dir_path.name))

        conf_file = open(conf_file_name, 'wt')
        conf_file.write(self.CONFIG_TEMPLATE.format(data_dir_path.name,
                                                    cache_dir_path.name,
                                                    state_dir_path.name))
        conf_file.close()

        self.modify_gitignore(cache_dir_path.name)

        if self.skip_git_actions:
            self.not_committed_changes_warning()
            return 0

        message = 'DVC init. data dir {}, cache dir {}, state dir {}'.format(
                        data_dir_path.name,
                        cache_dir_path.name,
                        state_dir_path.name
        )
        self.git.commit_all_changes_and_log_status(message)
        pass

    def modify_gitignore(self, cache_dir_name):
        gitignore_file = os.path.join(self.git.git_dir, '.gitignore')
        if not os.path.exists(gitignore_file):
            open(gitignore_file, 'a').close()
            Logger.printing('File .gitignore was created')
        with open(gitignore_file, 'a') as fd:
            fd.write('\n{}'.format(cache_dir_name))
            fd.write('\n{}'.format(os.path.basename(self.git.lock_file)))

        Logger.printing('Directory {} was added to .gitignore file'.format(cache_dir_name))

if __name__ == '__main__':
    run(CmdInit())
