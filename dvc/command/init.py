import os
from pathlib import Path

from dvc.command.base import CmdBase
from dvc.logger import Logger
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.state_file import StateFile
from dvc.system import System


class InitError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Init error: {}'.format(msg))


class CmdInit(CmdBase):
    CONFIG_TEMPLATE = '''[Global]
# Default target
Target = 

# Supported clouds: AWS, GCP
Cloud = AWS

# This global storage path is going to be deprecated in the next version.
# Please use StoragePath from a specific cloud instead.
#StoragePath = 

# Log levels: Debug, Info, Warning and Error
LogLevel = Info

[AWS]
CredentialPath = ~/.aws/credentials
CredentialSection = default

StoragePath = dvc/tutorial

# Default settings for AWS instances:
Type = t2.nano
Image = ami-2d39803a

SpotPrice = 
SpotTimeout = 300

KeyPairName = dvc-key
KeyPairDir = ~/.ssh
SecurityGroup = dvc-sg

Region = us-east-1
Zone = us-east-1a
SubnetId = 

Volume = my-100gb-drive-io

Monitoring = false
EbsOptimized = false
AllDisksAsRAID0 = false

[GCP]
StoragePath = 
ProjectName = 
'''

    EMPTY_FILE_NAME = '.empty'
    EMPTY_FILE_CHECKSUM = '0000000'

    def __init__(self, settings):
        super(CmdInit, self).__init__(settings)

    def get_not_existing_path(self, *args):
        path = Path(os.path.join(self.git.git_dir, *args))
        if path.exists():
            raise InitError('Path "{}" already exist'.format(path.name))
        return path

    def get_not_existing_conf_file_name(self):
        file_name = os.path.join(self.git.git_dir, Config.CONFIG_DIR, Config.CONFIG)
        if os.path.exists(file_name):
            raise InitError('Configuration file "{}" already exist'.format(file_name))
        return file_name

    def run(self):
        if not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        if os.path.realpath(os.path.curdir) != self.settings.git.git_dir_abs:
            Logger.error('DVC error: initialization could be done only from git root directory {}'.format(
                self.settings.git.git_dir_abs
            ))
            return 1

        config_dir_path = self.get_not_existing_path(Config.CONFIG_DIR)
        cache_dir_path = self.get_not_existing_path(Config.CONFIG_DIR, Config.CACHE_DIR)
        state_dir_path = self.get_not_existing_path(Config.CONFIG_DIR, Config.STATE_DIR)

        conf_file_name = self.get_not_existing_conf_file_name()

        config_dir_path.mkdir()
        cache_dir_path.mkdir()
        state_dir_path.mkdir()
        Logger.info('Directories {}/, {}/, {}/ were created'.format(
            config_dir_path.name,
            os.path.join(config_dir_path.name, cache_dir_path.name),
            os.path.join(config_dir_path.name, state_dir_path.name)))

        self.create_empty_file()

        conf_file = open(conf_file_name, 'wt')
        conf_file.write(self.CONFIG_TEMPLATE)
        conf_file.close()

        self.git.modify_gitignore([os.path.join(config_dir_path.name, cache_dir_path.name),
                                   os.path.join(config_dir_path.name, os.path.basename(self.git.lock_file))])

        message = 'DVC init. cache dir {}, state dir {}, '.format(cache_dir_path.name, state_dir_path.name)
        return self.commit_if_needed(message)

    def create_empty_file(self):
        empty_data_path = os.path.join(self.settings.git.git_dir_abs, self.EMPTY_FILE_NAME)

        data_item = self.settings.path_factory.data_item(empty_data_path)
        open(empty_data_path, 'w').close()
        data_item.move_data_to_cache()

        StateFile(StateFile.COMMAND_EMPTY_FILE,
                  data_item,
                  self.settings,
                  input_files=[],
                  output_files=[],
                  lock=False).save(is_update_target_metrics=False)
        pass
