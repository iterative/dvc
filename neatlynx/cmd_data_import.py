import sys
import os
from shutil import copyfile
import re
import fasteners
import requests

from neatlynx.cmd_base import CmdBase
from neatlynx.cmd_data_sync import sizeof_fmt
from neatlynx.logger import Logger
from neatlynx.data_file_obj import DataFileObj
from neatlynx.exceptions import NeatLynxException
from neatlynx.state_file import StateFile


class DataImportError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Import error: {}'.format(msg))


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
            Logger.printing('Cannot perform the command since NLX is busy and locked. Please retry the command later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            output = self.args.output
            for file in self.args.input:
                self.import_file(file, output, self.args.is_reproducible)

            if self.skip_git_actions:
                self.not_committed_changes_warning()
                return 0

            message = 'NLX data import: {} {}'.format(' '.join(self.args.input), self.args.output)
            self.git.commit_all_changes_and_log_status(message)
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

        dobj = DataFileObj(output, self.git, self.config)

        if os.path.exists(dobj.data_file_relative):
            raise DataImportError('Output file "{}" already exists'.format(dobj.data_file_relative))
        if not os.path.isdir(os.path.dirname(dobj.data_file_abs)):
            raise DataImportError('Output file directory "{}" does not exists'.format(
                os.path.dirname(dobj.data_file_relative)))

        cache_dir = os.path.dirname(dobj.cache_file_relative)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        if CmdDataImport.is_url(input):
            Logger.debug('Downloading file {} ...'.format(input))
            self.download_file(input, dobj.cache_file_relative)
            Logger.debug('Input file "{}" was downloaded to cache "{}"'.format(
                input, dobj.cache_file_relative))
        else:
            copyfile(input, dobj.cache_file_relative)
            Logger.debug('Input file "{}" was copied to cache "{}"'.format(
                input, dobj.cache_file_relative))

        dobj.create_symlink()
        Logger.debug('Symlink from data file "{}" to the cache file "{}" was created'.
                     format(dobj.data_file_relative, dobj.cache_file_relative))

        state_file = StateFile(dobj.state_file_relative,
                               self.git,
                               [input],
                               [output],
                               [],
                               is_reproducible)
        state_file.save()
        Logger.debug('State file "{}" was created'.format(dobj.state_file_relative))
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
    try:
        sys.exit(CmdDataImport().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
