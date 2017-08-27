import os
import re

try:
    from urlparse import urlparse
except ImportError:
    # Python 3
    from urllib.parse import urlparse

from dvc.command.base import CmdBase, DvcLock
from dvc.data_cloud import sizeof_fmt, file_md5
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.state_file import StateFile
from dvc.system import System

class ImportFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Import file: {}'.format(msg))


class CmdImportFile(CmdBase):
    def __init__(self, settings):
        super(CmdImportFile, self).__init__(settings)

    def run(self):
        targets = []
        with DvcLock(self.is_locker, self.git):
            output = self.parsed_args.output
            self.verify_output(output, self.parsed_args.input)

            for input in self.parsed_args.input:
                if not os.path.isdir(input):
                    targets.append((input, output))
                else:
                    input_dir = os.path.basename(input)
                    for root, dirs, files in os.walk(input):
                        for file in files:
                            filename = os.path.join(root, file)

                            rel = os.path.relpath(filename, input)
                            out = os.path.join(output, input_dir, rel)

                            out_dir = os.path.dirname(out)
                            if not os.path.exists(out_dir):
                                os.mkdir(out_dir)

                            targets.append((filename, out))
                pass
            self.import_files(targets, self.parsed_args.lock, self.parsed_args.jobs)
            message = 'DVC import files: {} -> {}'.format(str(self.parsed_args.input), output)
            self.commit_if_needed(message)
        pass

    @staticmethod
    def verify_output(output, input):
        if CmdImportFile.is_dir_path(output) and not os.path.isdir(output):
            raise ImportFileError(u'output directory {} does not exist'.format(output))

        if len(input) > 1 and not os.path.isdir(output):
            msg = u'output {} has to be directory for multiple file import'
            raise ImportFileError(msg.format(output))
        pass

    @staticmethod
    def is_dir_path(output):
        return len(output) > 0 and output[-1] == os.path.sep

    def import_and_commit_if_needed(self, input, output, lock=False, check_if_ready=True):
        if check_if_ready and not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        self.import_file(input, output, lock)

        message = 'DVC import file: {} {}'.format(input, output)
        return self.commit_if_needed(message)

    def import_file(self, input, output, lock):
        return self.import_files([(input, output)], lock)

    def collect_targets(self, targets):
        """
        Collect input, output and data_item's into tuples
        to be able to download them simulteniously later.
        """
        data_targets = []

        for target in targets:
            input = target[0]
            output = target[1]
            if not CmdImportFile.is_url(input):
                if not os.path.exists(input):
                    raise ImportFileError('Input file "{}" does not exist'.format(input))
                if not os.path.isfile(input):
                    raise ImportFileError('Input file "{}" has to be a regular file'.format(input))

            if os.path.isdir(output):
                output = os.path.join(output, os.path.basename(input))

            data_item = self.settings.path_factory.data_item(output)

            if os.path.exists(data_item.data.relative):
                raise ImportFileError('Output file "{}" already exists'.format(data_item.data.relative))
            if not os.path.isdir(os.path.dirname(data_item.data.relative)):
                raise ImportFileError('Output file directory "{}" does not exists'.format(
                    os.path.dirname(data_item.data.relative)))

            cache_dir = os.path.dirname(data_item.cache.relative)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)

            data_targets.append((input, data_item))

        return data_targets

    def import_files(self, targets, lock=False, jobs=1):
        data_targets = self.collect_targets(targets)
        imported_targets = self.cloud.import_data(data_targets, jobs)
        self.create_state_files(imported_targets, lock)

    def create_state_files(self, targets, lock):
        """
        Create state files for all targets.
        """
        for t in targets:
            orig_target, processed_data_item = t
            input, data_item  = orig_target
            output = data_item.data.relative

            if processed_data_item == None:
                Logger.debug('Skipping creating state file for failed import {}'.format(data_item.state.relative))
                continue

            Logger.debug('Creating symlink {} --> {}'.format(data_item.symlink_file, data_item.data.relative))
            System.symlink(data_item.symlink_file, data_item.data.relative)

            state_file = StateFile(StateFile.COMMAND_IMPORT_FILE,
                               data_item.state.relative,
                               self.settings,
                               argv=[input, output],
                               input_files=[],
                               output_files=[output],
                               lock=lock)
            state_file.save()
            Logger.debug('State file "{}" was created'.format(data_item.state.relative))

    @staticmethod
    def is_url(url):
        return len(urlparse(url).scheme) != 0
