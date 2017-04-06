import os
from shutil import copyfile
import re
import fasteners
import requests

from dvc.command.base import CmdBase
from dvc.command.data_sync import sizeof_fmt
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.state_file import StateFile
from dvc.utils import run


class DataImportError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Import error: {}'.format(msg))


class CmdDataImport(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)
        pass

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        parser.add_argument('input',
                            metavar='',
                            help='Input file',
                            nargs='*')

        self.add_string_arg(parser, 'output', 'Output file')

        parser.add_argument('-i', '--is-reproducible', action='store_false', default=False,
                            help='Is data file reproducible')
        pass

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.info('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            output = self.parsed_args.output
            for file in self.parsed_args.input:
                self.import_file(file, output, self.parsed_args.is_reproducible)

            message = 'DVC data import: {} {}'.format(' '.join(self.parsed_args.input), self.parsed_args.output)
            return self.commit_if_needed(message)
        finally:
            lock.release()
        pass

    def import_file(self, input, output, is_reproducible):
        if not CmdDataImport.is_url(input):
            if not os.path.exists(input):
                raise DataImportError('Input file "{}" does not exist'.format(input))
            if not os.path.isfile(input):
                raise DataImportError('Input file "{}" has to be a regular file'.format(input))

        if os.path.isdir(output):
            output = os.path.join(output, os.path.basename(input))

        data_item = self.path_factory.data_item(output)

        if os.path.exists(data_item.data.relative):
            raise DataImportError('Output file "{}" already exists'.format(data_item.data.relative))
        if not os.path.isdir(os.path.dirname(data_item.data.relative)):
            raise DataImportError('Output file directory "{}" does not exists'.format(
                os.path.dirname(data_item.data.relative)))

        cache_dir = os.path.dirname(data_item.cache.relative)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        if CmdDataImport.is_url(input):
            Logger.debug('Downloading file {} ...'.format(input))
            self.download_file(input, data_item.cache.relative)
            Logger.debug('Input file "{}" was downloaded to cache "{}"'.format(
                input, data_item.cache.relative))
        else:
            copyfile(input, data_item.cache.relative)
            Logger.debug('Input file "{}" was copied to cache "{}"'.format(
                input, data_item.cache.relative))

        data_item.create_symlink()
        Logger.debug('Symlink from data file "{}" to the cache file "{}" was created'.
                     format(data_item.data.relative, data_item.cache.relative))

        state_file = StateFile(data_item.state.relative,
                               self.git,
                               [],
                               [output],
                               [],
                               is_reproducible)
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
        return CmdDataImport.URL_REGEX.match(url) is not None

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
    run(CmdDataImport())
