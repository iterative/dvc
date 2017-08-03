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
DataDir = {}
CacheDir = {}
StateDir = {}
TargetFile = {}

# Supported clouds: AWS, GCP
Cloud = AWS
StoragePath =

# Log levels: Debug, Info, Warning and Error
LogLevel = Info

[AWS]
CredentialPath =

StoragePath = dvc/tutorial

DefaultKeyName = dvc-key

DefaultSecurityGroup = dvc-sg

#DefaultInstanceType = t2.nano
DefaultInstanceType = t2.nano

DefaultImage = ami-2d39803a

DefaultStorage = my-100gb-drive-io

DefaultRegion = us-east-1
DefaultZone = us-east-1a

SpotPrice =
SpotTimeout = 300

KeyDir = ~/.ssh

[GCP]
StoragePath =
ProjectName =
'''

    EMPTY_FILE_NAME = 'empty'
    EMPTY_FILE_CHECKSUM = '0000000'

    def __init__(self, settings):
        super(CmdInit, self).__init__(settings)

    def get_not_existing_path(self, dir):
        path = Path(os.path.join(self.git.git_dir, dir))
        if path.exists():
            raise InitError('Path "{}" already exist'.format(path.name))
        return path

    def get_not_existing_conf_file_name(self):
        file_name = os.path.join(self.git.git_dir, Config.CONFIG)
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

        data_dir_path = self.get_not_existing_path(self.parsed_args.data_dir)
        cache_dir_path = self.get_not_existing_path(self.parsed_args.cache_dir)
        state_dir_path = self.get_not_existing_path(self.parsed_args.state_dir)
        target_file_path = self.get_not_existing_path(self.parsed_args.target_file)

        self.settings.config.set(self.parsed_args.data_dir,
                                 self.parsed_args.cache_dir,
                                 self.parsed_args.state_dir,
                                 target_file_path)

        conf_file_name = self.get_not_existing_conf_file_name()

        data_dir_path.mkdir()
        cache_dir_path.mkdir()
        state_dir_path.mkdir()
        target_file_path.touch()
        Logger.info('Directories {}/, {}/, {}/ and target file {} were created'.format(
            data_dir_path.name,
            cache_dir_path.name,
            state_dir_path.name,
            target_file_path.name))

        self.create_empty_file()

        conf_file = open(conf_file_name, 'wt')
        conf_file.write(self.CONFIG_TEMPLATE.format(data_dir_path.name,
                                                    cache_dir_path.name,
                                                    state_dir_path.name,
                                                    target_file_path.name))
        conf_file.close()

        message = 'DVC init. data dir {}, cache dir {}, state dir {}, '.format(
                        data_dir_path.name,
                        cache_dir_path.name,
                        state_dir_path.name
        )
        if self.commit_if_needed(message) == 1:
            return 1

        self.modify_gitignore(cache_dir_path.name)
        return self.commit_if_needed('DVC init. Commit .gitignore file')

    def create_empty_file(self):
        empty_data_path = os.path.join(self.parsed_args.data_dir, self.EMPTY_FILE_NAME)
        cache_file_suffix = self.EMPTY_FILE_NAME + '_' + self.EMPTY_FILE_CHECKSUM
        empty_cache_path = os.path.join(self.parsed_args.cache_dir, cache_file_suffix)
        empty_state_path = os.path.join(self.parsed_args.state_dir, self.EMPTY_FILE_NAME + '.state')

        open(empty_cache_path, 'w').close()
        System.symlink(os.path.join('..', empty_cache_path), empty_data_path)

        StateFile(StateFile.COMMAND_EMPTY_FILE,
                  empty_state_path,
                  self.settings,
                  input_files=[],
                  output_files=[],
                  lock=False).save()
        pass

    def modify_gitignore(self, cache_dir_name):
        gitignore_file = os.path.join(self.git.git_dir, '.gitignore')
        if not os.path.exists(gitignore_file):
            open(gitignore_file, 'a').close()
            Logger.info('File .gitignore was created')
        with open(gitignore_file, 'a') as fd:
            fd.write('\n{}'.format(cache_dir_name))
            fd.write('\n{}'.format(os.path.basename(self.git.lock_file)))

        Logger.info('Directory {} was added to .gitignore file'.format(cache_dir_name))
