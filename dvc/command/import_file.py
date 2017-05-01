import os
from shutil import copyfile
import re
import fasteners
import requests

from dvc.command.base import CmdBase
from dvc.command.data_sync import sizeof_fmt
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.runtime import Runtime
from dvc.state_file import StateFile
from dvc.system import System


class ImportFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Import file: {}'.format(msg))


class CmdImportFile(CmdBase):
    def __init__(self, settings):
        super(CmdImportFile, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)

        self.add_string_arg(parser, 'input', 'Input file.')
        self.add_string_arg(parser, 'output', 'Output file.')
        pass

    def run(self):
        if self.is_locker:
            lock = fasteners.InterProcessLock(self.git.lock_file)
            gotten = lock.acquire(timeout=5)
            if not gotten:
                Logger.info('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
                return 1

        try:
            return self.import_and_commit_if_needed(self.parsed_args.input,
                                                    self.parsed_args.output,
                                                    self.parsed_args.lock)
        finally:
            if self.is_locker:
                lock.release()
        pass

    def import_and_commit_if_needed(self, input, output, lock=False, check_if_ready=True):
        if check_if_ready and not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        self.import_file(input, output, lock)

        message = 'DVC import file: {} {}'.format(input, output)
        return self.commit_if_needed(message)

    def import_file(self, input, output, lock=False):
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

        if CmdImportFile.is_url(input):
            Logger.debug('Downloading file {} ...'.format(input))
            self.download_file(input, data_item.cache.relative)
            Logger.debug('Input file "{}" was downloaded to cache "{}"'.format(
                input, data_item.cache.relative))
        else:
            copyfile(input, data_item.cache.relative)
            Logger.debug('Input file "{}" was copied to cache "{}"'.format(
                input, data_item.cache.relative))

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
        pass

    URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    @staticmethod
    def is_url(url):
        return CmdImportFile.URL_REGEX.match(url) is not None

    @staticmethod
    def download_file(from_url, to_file):
        r = requests.get(from_url, stream=True)

        chunk_size = 1024 * 100
        downloaded = 0
        last_reported = 0
        report_bucket = 100*1024*1024
        with open(to_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    downloaded += chunk_size
                    last_reported += chunk_size
                    if last_reported >= report_bucket:
                        last_reported = 0
                        Logger.debug('Downloaded {}'.format(sizeof_fmt(downloaded)))
                    f.write(chunk)
        return

if __name__ == '__main__':
    Runtime.run(CmdDataImport)
